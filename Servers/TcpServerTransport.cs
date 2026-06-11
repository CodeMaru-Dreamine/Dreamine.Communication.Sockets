using System.Collections.Concurrent;
using System.IO;
using System.Net;
using System.Net.Sockets;
using Dreamine.Communication.Abstractions.Enums;
using Dreamine.Communication.Abstractions.Interfaces;
using Dreamine.Communication.Abstractions.Models;
using Dreamine.Communication.Core.Framing;
using Dreamine.Communication.Core.Protocols;
using Dreamine.Communication.Sockets.Enums;
using Dreamine.Communication.Sockets.Options;

namespace Dreamine.Communication.Sockets.Servers;

/// <summary>
/// TCP 서버 기반 메시지 전송 계층입니다.
/// </summary>
/// <remarks>
/// 이 구현은 TCP 서버 수신 대기, 클라이언트 Accept, 메시지 수신,
/// 연결된 클라이언트 대상 Broadcast, FirstClient, LastClient 송신을 제공합니다.
/// </remarks>
public sealed class TcpServerTransport : IMessageTransport, IServerTransportMonitor
{
    private readonly TcpServerTransportOptions _options;
    private readonly IMessageProtocolAdapter _protocolAdapter;
    private readonly IMessageFrameCodec _frameCodec;
    private readonly ConcurrentDictionary<Guid, TcpClientConnectionEntry> _clients = new();

    private TcpListener? _listener;
    private CancellationTokenSource? _serverCts;
    private Task? _acceptLoopTask;
    private int _state = (int)ConnectionState.Disconnected;

    /// <summary>
    /// TcpServerTransport 클래스의 새 인스턴스를 초기화합니다.
    /// </summary>
    /// <param name="options">TCP 서버 설정입니다.</param>
    public TcpServerTransport(TcpServerTransportOptions options)
        : this(
            options,
            new DreamineEnvelopeProtocolAdapter(),
            new LengthPrefixedMessageFrameCodec())
    {
    }

    /// <summary>
    /// TcpServerTransport 클래스의 새 인스턴스를 초기화합니다.
    /// </summary>
    /// <param name="options">TCP 서버 설정입니다.</param>
    /// <param name="protocolAdapter">메시지 프로토콜 어댑터입니다.</param>
    /// <param name="frameCodec">메시지 프레임 코덱입니다.</param>
    public TcpServerTransport(
        TcpServerTransportOptions options,
        IMessageProtocolAdapter protocolAdapter,
        IMessageFrameCodec frameCodec)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _protocolAdapter = protocolAdapter ?? throw new ArgumentNullException(nameof(protocolAdapter));
        _frameCodec = frameCodec ?? throw new ArgumentNullException(nameof(frameCodec));

        ValidateOptions(_options);
    }

    /// <summary>
    /// 현재 연결 상태를 가져옵니다.
    /// </summary>
    public ConnectionState State => (ConnectionState)Volatile.Read(ref _state);

    /// <summary>
    /// 전송 방식 종류를 가져옵니다.
    /// </summary>
    public TransportKind Kind => TransportKind.Tcp;

    /// <summary>
    /// 현재 서버에 연결되어 있는 TCP 클라이언트 수를 가져옵니다.
    /// </summary>
    public int ConnectedClientCount => _clients.Count;

    /// <summary>
    /// 서버 SendAsync 호출 시 사용할 기본 송신 대상 정책을 가져오거나 설정합니다.
    /// </summary>
    public TcpServerSendTargetMode SendTargetMode
    {
        get => _options.SendTargetMode;
        set => _options.SendTargetMode = value;
    }

    /// <summary>
    /// 메시지를 수신했을 때 발생합니다.
    /// </summary>
    public event EventHandler<MessageEnvelope>? MessageReceived;

    /// <summary>
    /// 서버에 연결된 클라이언트 수가 변경될 때 발생합니다.
    /// </summary>
    public event EventHandler<int>? ConnectedClientCountChanged;

    /// <summary>
    /// TCP 서버 수신 대기를 시작합니다.
    /// </summary>
    /// <param name="cancellationToken">취소 토큰입니다.</param>
    public Task ConnectAsync(CancellationToken cancellationToken = default)
    {
        if (State is ConnectionState.Listening or ConnectionState.Connecting)
        {
            return Task.CompletedTask;
        }

        cancellationToken.ThrowIfCancellationRequested();

        SetState(ConnectionState.Connecting);

        try
        {
            CleanupListener();
            CleanupClients();

            var ipAddress = ParseHost(_options.Host);
            _listener = new TcpListener(ipAddress, _options.Port);
            _listener.Start(_options.Backlog);

            _serverCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

            SetState(ConnectionState.Listening);

            _acceptLoopTask = Task.Run(
                () => AcceptLoopAsync(_serverCts.Token),
                _serverCts.Token);

            return Task.CompletedTask;
        }
        catch
        {
            SetState(ConnectionState.Faulted);
            CleanupListener();
            CleanupClients();

            _serverCts?.Dispose();
            _serverCts = null;

            throw;
        }
    }

    /// <summary>
    /// TCP 서버 수신 대기를 중지하고 연결된 클라이언트를 모두 해제합니다.
    /// </summary>
    /// <param name="cancellationToken">취소 토큰입니다.</param>
    public async Task DisconnectAsync(CancellationToken cancellationToken = default)
    {
        if (State == ConnectionState.Disconnected)
        {
            return;
        }

        SetState(ConnectionState.Disconnecting);

        _serverCts?.Cancel();

        CleanupClients();
        CleanupListener();

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

        SetState(ConnectionState.Disconnected);
    }

    /// <summary>
    /// TCP 서버 설정의 SendTargetMode 정책에 따라 연결된 클라이언트에게 메시지를 전송합니다.
    /// </summary>
    /// <param name="message">전송할 메시지입니다.</param>
    /// <param name="cancellationToken">취소 토큰입니다.</param>
    public Task SendAsync(
        MessageEnvelope message,
        CancellationToken cancellationToken = default)
    {
        return SendAsync(SendTargetMode, message, cancellationToken);
    }

    /// <summary>
    /// 지정한 서버 송신 대상 정책에 따라 연결된 클라이언트에게 메시지를 전송합니다.
    /// </summary>
    /// <param name="targetMode">송신 대상 정책입니다.</param>
    /// <param name="message">전송할 메시지입니다.</param>
    /// <param name="cancellationToken">취소 토큰입니다.</param>
    public async Task SendAsync(
        TcpServerSendTargetMode targetMode,
        MessageEnvelope message,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(message);

        if (State != ConnectionState.Listening)
        {
            throw new InvalidOperationException("TCP server is not listening.");
        }

        var payload = _protocolAdapter.Encode(message);
        var targets = GetTargetClients(targetMode);

        if (targets.Length == 0)
        {
            throw new InvalidOperationException("No TCP client is connected to the server.");
        }

        var sendTasks = targets
            .Select(target => SendToClientAsync(target, payload, cancellationToken))
            .ToArray();

        await Task.WhenAll(sendTasks).ConfigureAwait(false);
    }

    /// <summary>
    /// 연결된 모든 TCP 클라이언트에게 메시지를 전송합니다.
    /// </summary>
    /// <param name="message">전송할 메시지입니다.</param>
    /// <param name="cancellationToken">취소 토큰입니다.</param>
    public Task BroadcastAsync(
        MessageEnvelope message,
        CancellationToken cancellationToken = default)
    {
        return SendAsync(TcpServerSendTargetMode.Broadcast, message, cancellationToken);
    }


    private TcpClientConnectionEntry[] GetTargetClients(TcpServerSendTargetMode targetMode)
    {
        var entries = _clients.Values
            .OrderBy(x => x.ConnectedAt)
            .ToArray();

        return targetMode switch
        {
            TcpServerSendTargetMode.Broadcast => entries,
            TcpServerSendTargetMode.FirstClient => entries.Take(1).ToArray(),
            TcpServerSendTargetMode.LastClient => entries.Reverse().Take(1).ToArray(),
            _ => entries
        };
    }

    /// <summary>
    /// TCP 서버 리소스를 비동기로 해제합니다.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        await DisconnectAsync().ConfigureAwait(false);
    }

    private async Task SendToClientAsync(
        TcpClientConnectionEntry target,
        byte[] payload,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (!target.Client.Connected)
        {
            RemoveClient(target.ClientId, target.Client);
            return;
        }

        await target.SendLock.WaitAsync(cancellationToken).ConfigureAwait(false);

        try
        {
            if (!target.Client.Connected)
            {
                RemoveClient(target.ClientId, target.Client);
                return;
            }

            var stream = target.Client.GetStream();

            await _frameCodec.WriteFrameAsync(stream, payload, cancellationToken)
                .ConfigureAwait(false);
        }
        catch
        {
            RemoveClient(target.ClientId, target.Client);
        }
        finally
        {
            target.SendLock.Release();
        }
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
                   State == ConnectionState.Listening)
            {
                var client = await _listener.AcceptTcpClientAsync(cancellationToken)
                    .ConfigureAwait(false);

                client.ReceiveBufferSize = _options.ReceiveBufferSize;
                client.SendBufferSize = _options.SendBufferSize;

                var clientId = Guid.NewGuid();
                var entry = new TcpClientConnectionEntry(
                    clientId,
                    client,
                    DateTimeOffset.UtcNow);

                _clients[clientId] = entry;
                NotifyConnectedClientCountChanged();

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
                SetState(ConnectionState.Faulted);
                CleanupListener();
                CleanupClients();
            }
        }
        catch (IOException)
        {
            if (!cancellationToken.IsCancellationRequested)
            {
                SetState(ConnectionState.Faulted);
                CleanupListener();
                CleanupClients();
            }
        }
        catch
        {
            if (!cancellationToken.IsCancellationRequested)
            {
                SetState(ConnectionState.Faulted);
                CleanupListener();
                CleanupClients();
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
                   State == ConnectionState.Listening &&
                   client.Connected)
            {
                var payload = await _frameCodec.ReadFrameAsync(stream, cancellationToken)
                    .ConfigureAwait(false);

                if (payload is null)
                {
                    break;
                }

                var message = _protocolAdapter.Decode(payload);
                MessageReceived?.Invoke(this, message);
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
        }
        catch
        {
            // 수신 루프 단위 예외는 해당 클라이언트 제거로 처리합니다.
        }
        finally
        {
            RemoveClient(clientId, client);
        }
    }

    private void CleanupListener()
    {
        try
        {
            _listener?.Stop();
        }
        catch
        {
            // Ignore cleanup exceptions.
        }

        _listener = null;
    }

    private void CleanupClients()
    {
        foreach (var pair in _clients.ToArray())
        {
            RemoveClient(pair.Key, pair.Value.Client);
        }
    }

    private void RemoveClient(Guid clientId, TcpClient client)
    {
        var removed = _clients.TryRemove(clientId, out _);

        try
        {
            client.Close();
            client.Dispose();
        }
        catch
        {
            // Ignore cleanup exceptions.
        }

        if (removed)
        {
            NotifyConnectedClientCountChanged();
        }
    }

    private void SetState(ConnectionState state)
    {
        Interlocked.Exchange(ref _state, (int)state);
    }

    private void NotifyConnectedClientCountChanged()
    {
        ConnectedClientCountChanged?.Invoke(this, ConnectedClientCount);
    }

    private static IPAddress ParseHost(string host)
    {
        if (host == "0.0.0.0")
        {
            return IPAddress.Any;
        }

        if (host == "127.0.0.1" ||
            host.Equals("localhost", StringComparison.OrdinalIgnoreCase))
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

    private sealed class TcpClientConnectionEntry
    {
        public TcpClientConnectionEntry(
            Guid clientId,
            TcpClient client,
            DateTimeOffset connectedAt)
        {
            ClientId = clientId;
            Client = client ?? throw new ArgumentNullException(nameof(client));
            ConnectedAt = connectedAt;
        }

        public Guid ClientId { get; }

        public TcpClient Client { get; }

        public DateTimeOffset ConnectedAt { get; }

        public SemaphoreSlim SendLock { get; } = new(1, 1);
    }

}
