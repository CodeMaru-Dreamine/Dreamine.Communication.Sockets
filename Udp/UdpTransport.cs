using System.Net;
using System.Net.Sockets;
using Dreamine.Communication.Abstractions.Enums;
using Dreamine.Communication.Abstractions.Interfaces;
using Dreamine.Communication.Abstractions.Models;
using Dreamine.Communication.Core.Protocols;
using Dreamine.Communication.Sockets.Options;

namespace Dreamine.Communication.Sockets.Udp;

/// <summary>
/// \brief UDP 기반 메시지 전송 계층입니다.
/// </summary>
/// <remarks>
/// UDP는 Datagram 기반 통신이므로 별도의 FrameCodec을 사용하지 않습니다.
/// 수신된 Datagram 단위를 그대로 ProtocolAdapter에 전달합니다.
/// </remarks>
public sealed class UdpTransport : IMessageTransport
{
    private readonly UdpTransportOptions _options;
    private readonly IMessageProtocolAdapter _protocolAdapter;

    private UdpClient? _client;
    private IPEndPoint? _remoteEndPoint;
    private CancellationTokenSource? _receiveLoopCts;
    private Task? _receiveLoopTask;

    /// <summary>
    /// \brief UdpTransport 클래스의 새 인스턴스를 초기화합니다.
    /// </summary>
    /// <param name="options">UDP 설정입니다.</param>
    public UdpTransport(UdpTransportOptions options)
        : this(options, new DreamineEnvelopeProtocolAdapter())
    {
    }

    /// <summary>
    /// \brief UdpTransport 클래스의 새 인스턴스를 초기화합니다.
    /// </summary>
    /// <param name="options">UDP 설정입니다.</param>
    /// <param name="protocolAdapter">메시지 프로토콜 어댑터입니다.</param>
    public UdpTransport(
        UdpTransportOptions options,
        IMessageProtocolAdapter protocolAdapter)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _protocolAdapter = protocolAdapter ?? throw new ArgumentNullException(nameof(protocolAdapter));

        ValidateOptions(_options);
    }

    /// <summary>
    /// \brief 현재 연결 상태를 가져옵니다.
    /// </summary>
    public ConnectionState State { get; private set; } = ConnectionState.Disconnected;

    /// <summary>
    /// \brief 전송 방식 종류를 가져옵니다.
    /// </summary>
    public TransportKind Kind => TransportKind.Udp;

    /// <summary>
    /// \brief 메시지를 수신했을 때 발생합니다.
    /// </summary>
    public event EventHandler<MessageEnvelope>? MessageReceived;

    /// <summary>
    /// \brief UDP 소켓을 열고 수신 루프를 시작합니다.
    /// </summary>
    /// <param name="cancellationToken">취소 토큰입니다.</param>
    public Task ConnectAsync(CancellationToken cancellationToken = default)
    {
        if (State is ConnectionState.Connected or ConnectionState.Connecting)
        {
            return Task.CompletedTask;
        }

        cancellationToken.ThrowIfCancellationRequested();

        State = ConnectionState.Connecting;

        try
        {
            var localEndPoint = _options.CreateLocalEndPoint();
            _remoteEndPoint = _options.CreateRemoteEndPoint();

            _client = new UdpClient(AddressFamily.InterNetwork)
            {
                EnableBroadcast = _options.EnableBroadcast
            };

            _client.Client.ReceiveBufferSize = _options.ReceiveBufferSize;
            _client.Client.SendBufferSize = _options.SendBufferSize;

            if (_options.ReuseAddress)
            {
                _client.Client.SetSocketOption(
                    SocketOptionLevel.Socket,
                    SocketOptionName.ReuseAddress,
                    true);
            }

            _client.Client.Bind(localEndPoint);

            _receiveLoopCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

            State = ConnectionState.Connected;

            _receiveLoopTask = Task.Run(
                () => ReceiveLoopAsync(_receiveLoopCts.Token),
                _receiveLoopCts.Token);

            return Task.CompletedTask;
        }
        catch
        {
            State = ConnectionState.Faulted;

            _client?.Dispose();
            _client = null;
            _remoteEndPoint = null;

            _receiveLoopCts?.Dispose();
            _receiveLoopCts = null;

            throw;
        }
    }

    /// <summary>
    /// \brief UDP 소켓을 닫고 수신 루프를 종료합니다.
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
        _remoteEndPoint = null;

        if (_receiveLoopTask is not null)
        {
            try
            {
                await _receiveLoopTask.ConfigureAwait(false);
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

        _receiveLoopCts?.Dispose();
        _receiveLoopCts = null;
        _receiveLoopTask = null;

        cancellationToken.ThrowIfCancellationRequested();

        State = ConnectionState.Disconnected;
    }

    /// <summary>
    /// \brief UDP 원격 엔드포인트로 메시지를 전송합니다.
    /// </summary>
    /// <param name="message">전송할 메시지입니다.</param>
    /// <param name="cancellationToken">취소 토큰입니다.</param>
    public async Task SendAsync(
        MessageEnvelope message,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(message);

        if (_client is null ||
            _remoteEndPoint is null ||
            State != ConnectionState.Connected)
        {
            throw new InvalidOperationException("UDP transport is not connected.");
        }

        cancellationToken.ThrowIfCancellationRequested();

        var payload = _protocolAdapter.Encode(message);

        await _client.SendAsync(payload, _remoteEndPoint, cancellationToken)
            .ConfigureAwait(false);
    }

    /// <summary>
    /// \brief UDP 전송 계층 리소스를 비동기로 해제합니다.
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
            while (!cancellationToken.IsCancellationRequested &&
                   State == ConnectionState.Connected)
            {
                var result = await _client.ReceiveAsync(cancellationToken)
                    .ConfigureAwait(false);

                var message = _protocolAdapter.Decode(result.Buffer);
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
            if (!cancellationToken.IsCancellationRequested)
            {
                State = ConnectionState.Faulted;
            }
        }
        catch
        {
            if (!cancellationToken.IsCancellationRequested)
            {
                State = ConnectionState.Faulted;
            }
        }
    }

    private static void ValidateOptions(UdpTransportOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentException.ThrowIfNullOrWhiteSpace(options.LocalHost);
        ArgumentException.ThrowIfNullOrWhiteSpace(options.RemoteHost);

        ValidatePort(options.LocalPort, nameof(options.LocalPort));
        ValidatePort(options.RemotePort, nameof(options.RemotePort));

        if (options.ReceiveBufferSize <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(options.ReceiveBufferSize));
        }

        if (options.SendBufferSize <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(options.SendBufferSize));
        }
    }

    private static void ValidatePort(int port, string parameterName)
    {
        if (port <= 0 || port > 65535)
        {
            throw new ArgumentOutOfRangeException(parameterName);
        }
    }
}
