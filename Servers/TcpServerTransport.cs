using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using Dreamine.Communication.Abstractions.Enums;
using Dreamine.Communication.Abstractions.Interfaces;
using Dreamine.Communication.Abstractions.Models;
using Dreamine.Communication.Core.Framing;
using Dreamine.Communication.Core.Serialization;
using Dreamine.Communication.Sockets.Options;

namespace Dreamine.Communication.Sockets.Servers;

/// <summary>
/// \brief TCP 서버 기반 메시지 전송 계층입니다.
/// </summary>
/// <remarks>
/// 이 구현은 1차 TCP Loopback 검증용 서버입니다.
/// SendAsync 호출 시 현재 연결된 모든 클라이언트에게 메시지를 Broadcast합니다.
/// </remarks>
public sealed class TcpServerTransport : IMessageTransport
{
    private readonly TcpServerTransportOptions _options;
    private readonly IMessageSerializer _serializer;
    private readonly IMessageFrameCodec _frameCodec;
    private readonly ConcurrentDictionary<Guid, TcpClient> _clients = new();
    private readonly SemaphoreSlim _sendLock = new(1, 1);

    private TcpListener? _listener;
    private CancellationTokenSource? _serverCts;
    private Task? _acceptLoopTask;

    /// <summary>
    /// \brief TcpServerTransport 클래스의 새 인스턴스를 초기화합니다.
    /// </summary>
    /// <param name="options">TCP 서버 설정입니다.</param>
    public TcpServerTransport(TcpServerTransportOptions options)
        : this(options, new JsonMessageSerializer(), new LengthPrefixedMessageFrameCodec())
    {
    }

    /// <summary>
    /// \brief TcpServerTransport 클래스의 새 인스턴스를 초기화합니다.
    /// </summary>
    /// <param name="options">TCP 서버 설정입니다.</param>
    /// <param name="serializer">메시지 직렬화기입니다.</param>
    /// <param name="frameCodec">메시지 프레임 코덱입니다.</param>
    public TcpServerTransport(
        TcpServerTransportOptions options,
        IMessageSerializer serializer,
        IMessageFrameCodec frameCodec)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
        _frameCodec = frameCodec ?? throw new ArgumentNullException(nameof(frameCodec));

        ValidateOptions(_options);
    }

    /// <summary>
    /// \brief 현재 연결 상태를 가져옵니다.
    /// </summary>
    public ConnectionState State { get; private set; } = ConnectionState.Disconnected;

    /// <summary>
    /// \brief 전송 방식 종류를 가져옵니다.
    /// </summary>
    public TransportKind Kind => TransportKind.Tcp;

    /// <summary>
    /// \brief 메시지를 수신했을 때 발생합니다.
    /// </summary>
    public event EventHandler<MessageEnvelope>? MessageReceived;

    /// <summary>
    /// \brief TCP 서버 수신 대기를 시작합니다.
    /// </summary>
    /// <param name="cancellationToken">취소 토큰입니다.</param>
    public Task ConnectAsync(CancellationToken cancellationToken = default)
    {
        if (State == ConnectionState.Connected || State == ConnectionState.Connecting)
        {
            return Task.CompletedTask;
        }

        cancellationToken.ThrowIfCancellationRequested();

        State = ConnectionState.Connecting;

        try
        {
            var ipAddress = ParseHost(_options.Host);
            _listener = new TcpListener(ipAddress, _options.Port);
            _listener.Start(_options.Backlog);

            _serverCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

            State = ConnectionState.Connected;

            _acceptLoopTask = Task.Run(
                () => AcceptLoopAsync(_serverCts.Token),
                _serverCts.Token);

            return Task.CompletedTask;
        }
        catch
        {
            State = ConnectionState.Faulted;
            _listener?.Stop();
            _listener = null;
            _serverCts?.Dispose();
            _serverCts = null;
            throw;
        }
    }

    /// <summary>
    /// \brief TCP 서버 수신 대기를 중지하고 연결된 클라이언트를 모두 해제합니다.
    /// </summary>
    /// <param name="cancellationToken">취소 토큰입니다.</param>
    public async Task DisconnectAsync(CancellationToken cancellationToken = default)
    {
        if (State == ConnectionState.Disconnected)
        {
            return;
        }

        State = ConnectionState.Disconnecting;

        _serverCts?.Cancel();

        foreach (var pair in _clients)
        {
            RemoveClient(pair.Key, pair.Value);
        }

        _listener?.Stop();
        _listener = null;

        if (_acceptLoopTask is not null)
        {
            try
            {
                await _acceptLoopTask.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
            }
            catch (ObjectDisposedException)
            {
            }
            catch (SocketException)
            {
            }
        }

        _serverCts?.Dispose();
        _serverCts = null;
        _acceptLoopTask = null;

        cancellationToken.ThrowIfCancellationRequested();

        State = ConnectionState.Disconnected;
    }

    /// <summary>
    /// \brief 연결된 모든 TCP 클라이언트에게 메시지를 전송합니다.
    /// </summary>
    /// <param name="message">전송할 메시지입니다.</param>
    /// <param name="cancellationToken">취소 토큰입니다.</param>
    public async Task SendAsync(
        MessageEnvelope message,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(message);

        if (State != ConnectionState.Connected)
        {
            throw new InvalidOperationException("TCP server is not running.");
        }

        var payload = _serializer.Serialize(message);

        await _sendLock.WaitAsync(cancellationToken).ConfigureAwait(false);

        try
        {
            foreach (var pair in _clients.ToArray())
            {
                cancellationToken.ThrowIfCancellationRequested();

                var clientId = pair.Key;
                var client = pair.Value;

                if (!client.Connected)
                {
                    RemoveClient(clientId, client);
                    continue;
                }

                try
                {
                    var stream = client.GetStream();
                    await _frameCodec.WriteFrameAsync(stream, payload, cancellationToken)
                        .ConfigureAwait(false);
                }
                catch
                {
                    RemoveClient(clientId, client);
                }
            }
        }
        finally
        {
            _sendLock.Release();
        }
    }

    /// <summary>
    /// \brief TCP 서버 리소스를 비동기로 해제합니다.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        await DisconnectAsync().ConfigureAwait(false);
        _sendLock.Dispose();
    }

    private async Task AcceptLoopAsync(CancellationToken cancellationToken)
    {
        if (_listener is null)
        {
            return;
        }

        try
        {
            while (!cancellationToken.IsCancellationRequested &&
                   State == ConnectionState.Connected)
            {
                var client = await _listener.AcceptTcpClientAsync(cancellationToken)
                    .ConfigureAwait(false);

                client.ReceiveBufferSize = _options.ReceiveBufferSize;
                client.SendBufferSize = _options.SendBufferSize;

                var clientId = Guid.NewGuid();
                _clients[clientId] = client;

                _ = Task.Run(
                    () => ReceiveLoopAsync(clientId, client, cancellationToken),
                    cancellationToken);
            }
        }
        catch (OperationCanceledException)
        {
        }
        catch (ObjectDisposedException)
        {
        }
        catch (SocketException)
        {
            if (!cancellationToken.IsCancellationRequested)
            {
                State = ConnectionState.Faulted;
                throw;
            }
        }
    }

    private async Task ReceiveLoopAsync(
        Guid clientId,
        TcpClient client,
        CancellationToken cancellationToken)
    {
        try
        {
            var stream = client.GetStream();

            while (!cancellationToken.IsCancellationRequested &&
                   State == ConnectionState.Connected &&
                   client.Connected)
            {
                var payload = await _frameCodec.ReadFrameAsync(stream, cancellationToken)
                    .ConfigureAwait(false);

                if (payload is null)
                {
                    break;
                }

                var message = _serializer.Deserialize(payload);
                MessageReceived?.Invoke(this, message);
            }
        }
        catch (OperationCanceledException)
        {
        }
        catch (ObjectDisposedException)
        {
        }
        catch
        {
            RemoveClient(clientId, client);
        }
        finally
        {
            RemoveClient(clientId, client);
        }
    }

    private void RemoveClient(Guid clientId, TcpClient client)
    {
        _clients.TryRemove(clientId, out _);

        try
        {
            client.Close();
            client.Dispose();
        }
        catch
        {
            // Ignore cleanup exceptions.
        }
    }

    private static IPAddress ParseHost(string host)
    {
        if (host == "0.0.0.0")
        {
            return IPAddress.Any;
        }

        if (host == "127.0.0.1" || host.Equals("localhost", StringComparison.OrdinalIgnoreCase))
        {
            return IPAddress.Loopback;
        }

        if (IPAddress.TryParse(host, out var ipAddress))
        {
            return ipAddress;
        }

        throw new ArgumentException($"Invalid TCP server host: {host}", nameof(host));
    }

    private static void ValidateOptions(TcpServerTransportOptions options)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(options.Host);

        if (options.Port <= 0 || options.Port > 65535)
        {
            throw new ArgumentOutOfRangeException(nameof(options.Port));
        }

        if (options.Backlog <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(options.Backlog));
        }

        if (options.ReceiveBufferSize <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(options.ReceiveBufferSize));
        }

        if (options.SendBufferSize <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(options.SendBufferSize));
        }
    }
}