using System;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Dreamine.Communication.Abstractions.Enums;
using Dreamine.Communication.Abstractions.Interfaces;
using Dreamine.Communication.Abstractions.Models;
using Dreamine.Communication.Core.Serialization;
using Dreamine.Communication.Core.Framing;
using Dreamine.Communication.Sockets.Options;

namespace Dreamine.Communication.Sockets.Clients;

/// <summary>
/// \brief TCP 클라이언트 기반 메시지 전송 계층입니다.
/// </summary>
public sealed class TcpClientTransport : IMessageTransport
{
    private readonly TcpClientTransportOptions _options;
    private readonly IMessageSerializer _serializer;
    private readonly IMessageFrameCodec _frameCodec;

    private TcpClient? _client;
    private CancellationTokenSource? _receiveLoopCts;
    private Task? _receiveLoopTask;

    /// <summary>
    /// \brief TcpClientTransport 클래스의 새 인스턴스를 초기화합니다.
    /// </summary>
    /// <param name="options">TCP 클라이언트 설정입니다.</param>
    public TcpClientTransport(TcpClientTransportOptions options)
        : this(options, new JsonMessageSerializer(), new LengthPrefixedMessageFrameCodec())
    {
    }

    /// <summary>
    /// \brief TcpClientTransport 클래스의 새 인스턴스를 초기화합니다.
    /// </summary>
    /// <param name="options">TCP 클라이언트 설정입니다.</param>
    /// <param name="serializer">메시지 직렬화기입니다.</param>
    /// <param name="frameCodec">메시지 프레임 코덱입니다.</param>
    public TcpClientTransport(
        TcpClientTransportOptions options,
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
    /// \brief TCP 서버에 연결합니다.
    /// </summary>
    /// <param name="cancellationToken">취소 토큰입니다.</param>
    public async Task ConnectAsync(CancellationToken cancellationToken = default)
    {
        if (State == ConnectionState.Connected || State == ConnectionState.Connecting)
        {
            return;
        }

        State = ConnectionState.Connecting;

        try
        {
            _client = new TcpClient
            {
                ReceiveBufferSize = _options.ReceiveBufferSize,
                SendBufferSize = _options.SendBufferSize
            };

            using var timeoutCts = new CancellationTokenSource(_options.ConnectTimeoutMs);
            using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(
                cancellationToken,
                timeoutCts.Token);

            await _client.ConnectAsync(_options.Host, _options.Port, linkedCts.Token)
                .ConfigureAwait(false);

            State = ConnectionState.Connected;

            _receiveLoopCts = new CancellationTokenSource();
            _receiveLoopTask = Task.Run(
                () => ReceiveLoopAsync(_receiveLoopCts.Token),
                _receiveLoopCts.Token);
        }
        catch
        {
            State = ConnectionState.Faulted;
            _client?.Dispose();
            _client = null;
            throw;
        }
    }

    /// <summary>
    /// \brief TCP 연결을 종료합니다.
    /// </summary>
    /// <param name="cancellationToken">취소 토큰입니다.</param>
    public async Task DisconnectAsync(CancellationToken cancellationToken = default)
    {
        if (State == ConnectionState.Disconnected)
        {
            return;
        }

        State = ConnectionState.Disconnecting;

        _receiveLoopCts?.Cancel();

        _client?.Close();
        _client?.Dispose();
        _client = null;

        if (_receiveLoopTask is not null)
        {
            try
            {
                await _receiveLoopTask.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
            }
        }

        _receiveLoopCts?.Dispose();
        _receiveLoopCts = null;
        _receiveLoopTask = null;

        cancellationToken.ThrowIfCancellationRequested();
        State = ConnectionState.Disconnected;
    }

    /// <summary>
    /// \brief 메시지를 TCP 서버로 전송합니다.
    /// </summary>
    /// <param name="message">전송할 메시지입니다.</param>
    /// <param name="cancellationToken">취소 토큰입니다.</param>
    public async Task SendAsync(
        MessageEnvelope message,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(message);

        if (_client is null || State != ConnectionState.Connected)
        {
            throw new InvalidOperationException("TCP client is not connected.");
        }

        var stream = _client.GetStream();
        var payload = _serializer.Serialize(message);

        await _frameCodec.WriteFrameAsync(stream, payload, cancellationToken)
            .ConfigureAwait(false);
    }

    /// <summary>
    /// \brief TCP 클라이언트 리소스를 비동기로 해제합니다.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        await DisconnectAsync().ConfigureAwait(false);
    }

    private async Task ReceiveLoopAsync(CancellationToken cancellationToken)
    {
        if (_client is null)
        {
            return;
        }

        try
        {
            var stream = _client.GetStream();

            while (!cancellationToken.IsCancellationRequested &&
                   State == ConnectionState.Connected)
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
        catch
        {
            State = ConnectionState.Faulted;
            throw;
        }
    }

    private static void ValidateOptions(TcpClientTransportOptions options)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(options.Host);

        if (options.Port <= 0 || options.Port > 65535)
        {
            throw new ArgumentOutOfRangeException(nameof(options.Port));
        }

        if (options.ReceiveBufferSize <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(options.ReceiveBufferSize));
        }

        if (options.SendBufferSize <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(options.SendBufferSize));
        }

        if (options.ConnectTimeoutMs <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(options.ConnectTimeoutMs));
        }
    }
}