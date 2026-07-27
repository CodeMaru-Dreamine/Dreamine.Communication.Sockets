using System.Net;
using System.Net.Sockets;
using Dreamine.Communication.Abstractions.Enums;
using Dreamine.Communication.Abstractions.Interfaces;
using Dreamine.Communication.Abstractions.Models;
using Dreamine.Communication.Core.Protocols;
using Dreamine.Communication.Sockets.Options;

namespace Dreamine.Communication.Sockets.Udp;

/// <summary>
/// \if KO
/// <para>UDP 데이터그램을 프로토콜 메시지로 변환해 송수신하는 전송 계층입니다.</para>
/// \endif
/// \if EN
/// <para>Sends and receives protocol messages as UDP datagrams.</para>
/// \endif
/// </summary>
/// <remarks>
/// \if KO
/// <para>UDP 데이터그램을 프로토콜 메시지로 변환해 송수신하는 전송 계층입니다.</para>
/// \endif
/// \if EN
/// <para>Sends and receives protocol messages as UDP datagrams.</para>
/// \endif
/// </remarks>
public sealed class UdpTransport : IMessageTransport
{
    /// <summary>
    /// \if KO
    /// <para>options 값을 보관합니다.</para>
    /// \endif
    /// \if EN
    /// <para>Stores the options value.</para>
    /// \endif
    /// </summary>
    private readonly UdpTransportOptions _options;
    /// <summary>
    /// \if KO
    /// <para>protocol Adapter 값을 보관합니다.</para>
    /// \endif
    /// \if EN
    /// <para>Stores the protocol adapter value.</para>
    /// \endif
    /// </summary>
    private readonly IMessageProtocolAdapter _protocolAdapter;

    /// <summary>
    /// \if KO
    /// <para>client 값을 보관합니다.</para>
    /// \endif
    /// \if EN
    /// <para>Stores the client value.</para>
    /// \endif
    /// </summary>
    private UdpClient? _client;
    /// <summary>
    /// \if KO
    /// <para>remote End Point 값을 보관합니다.</para>
    /// \endif
    /// \if EN
    /// <para>Stores the remote end point value.</para>
    /// \endif
    /// </summary>
    private IPEndPoint? _remoteEndPoint;
    /// <summary>
    /// \if KO
    /// <para>receive Loop Cts 값을 보관합니다.</para>
    /// \endif
    /// \if EN
    /// <para>Stores the receive loop cts value.</para>
    /// \endif
    /// </summary>
    private CancellationTokenSource? _receiveLoopCts;
    /// <summary>
    /// \if KO
    /// <para>receive Loop Task 값을 보관합니다.</para>
    /// \endif
    /// \if EN
    /// <para>Stores the receive loop task value.</para>
    /// \endif
    /// </summary>
    private Task? _receiveLoopTask;
    /// <summary>
    /// \if KO
    /// <para>state 값을 보관합니다.</para>
    /// \endif
    /// \if EN
    /// <para>Stores the state value.</para>
    /// \endif
    /// </summary>
    private int _state = (int)ConnectionState.Disconnected;

    /// <summary>
    /// \if KO
    /// <para>기본 Dreamine JSON 프로토콜로 UDP 전송 계층을 초기화합니다.</para>
    /// \endif
    /// \if EN
    /// <para>Initializes UDP transport with the default Dreamine JSON protocol.</para>
    /// \endif
    /// </summary>
    /// <param name="options">
    /// \if KO
    /// <para>로컬·원격 엔드포인트와 소켓 설정입니다.</para>
    /// \endif
    /// \if EN
    /// <para>The local and remote endpoint and socket options.</para>
    /// \endif
    /// </param>
    public UdpTransport(UdpTransportOptions options)
        : this(options, new DreamineEnvelopeProtocolAdapter())
    {
    }

    /// <summary>
    /// \if KO
    /// <para>UDP 설정과 사용자 지정 프로토콜 어댑터로 전송 계층을 초기화합니다.</para>
    /// \endif
    /// \if EN
    /// <para>Initializes UDP transport with options and a custom protocol adapter.</para>
    /// \endif
    /// </summary>
    /// <param name="options">
    /// \if KO
    /// <para>로컬·원격 엔드포인트와 소켓 설정입니다.</para>
    /// \endif
    /// \if EN
    /// <para>The local and remote endpoint and socket options.</para>
    /// \endif
    /// </param>
    /// <param name="protocolAdapter">
    /// \if KO
    /// <para>메시지와 데이터그램 페이로드를 변환할 어댑터입니다.</para>
    /// \endif
    /// \if EN
    /// <para>The adapter that converts messages and datagram payloads.</para>
    /// \endif
    /// </param>
    /// <exception cref="ArgumentNullException">
    /// \if KO
    /// <para><paramref name="options"/> 또는 <paramref name="protocolAdapter"/>가 <see langword="null"/>인 경우 발생합니다.</para>
    /// \endif
    /// \if EN
    /// <para>Thrown when <paramref name="options"/> or <paramref name="protocolAdapter"/> is <see langword="null"/>.</para>
    /// \endif
    /// </exception>
    public UdpTransport(
        UdpTransportOptions options,
        IMessageProtocolAdapter protocolAdapter)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _protocolAdapter = protocolAdapter ?? throw new ArgumentNullException(nameof(protocolAdapter));

        ValidateOptions(_options);
    }

    /// <summary>
    /// \if KO
    /// <para>스레드 안전하게 현재 UDP 소켓 상태를 가져옵니다.</para>
    /// \endif
    /// \if EN
    /// <para>Gets the current UDP socket state in a thread-safe manner.</para>
    /// \endif
    /// </summary>
    public ConnectionState State => (ConnectionState)Volatile.Read(ref _state);

    /// <summary>
    /// \if KO
    /// <para>UDP 전송 방식을 가져옵니다.</para>
    /// \endif
    /// \if EN
    /// <para>Gets the UDP transport kind.</para>
    /// \endif
    /// </summary>
    public TransportKind Kind => TransportKind.Udp;

    /// <summary>
    /// \if KO
    /// <para>수신 데이터그램을 메시지로 디코딩했을 때 발생합니다.</para>
    /// \endif
    /// \if EN
    /// <para>Occurs when a received datagram has been decoded into a message.</para>
    /// \endif
    /// </summary>
    public event EventHandler<MessageEnvelope>? MessageReceived;

    /// <summary>
    /// \if KO
    /// <para>로컬 엔드포인트에 UDP 소켓을 바인딩하고 백그라운드 수신 루프를 시작합니다.</para>
    /// \endif
    /// \if EN
    /// <para>Binds a UDP socket to the local endpoint and starts the background receive loop.</para>
    /// \endif
    /// </summary>
    /// <param name="cancellationToken">
    /// \if KO
    /// <para>연결 및 수신 루프 수명과 연결할 취소 토큰입니다.</para>
    /// \endif
    /// \if EN
    /// <para>A cancellation token linked to connection and receive-loop lifetime.</para>
    /// \endif
    /// </param>
    /// <returns>
    /// \if KO
    /// <para>UDP 소켓 시작 작업입니다.</para>
    /// \endif
    /// \if EN
    /// <para>A task representing UDP socket startup.</para>
    /// \endif
    /// </returns>
    public Task ConnectAsync(CancellationToken cancellationToken = default)
    {
        if (State is ConnectionState.Connected or ConnectionState.Connecting)
        {
            return Task.CompletedTask;
        }

        cancellationToken.ThrowIfCancellationRequested();

        SetState(ConnectionState.Connecting);

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

            SetState(ConnectionState.Connected);

            _receiveLoopTask = Task.Run(
                () => ReceiveLoopAsync(_receiveLoopCts.Token),
                _receiveLoopCts.Token);

            return Task.CompletedTask;
        }
        catch
        {
            SetState(ConnectionState.Faulted);

            _client?.Dispose();
            _client = null;
            _remoteEndPoint = null;

            _receiveLoopCts?.Dispose();
            _receiveLoopCts = null;

            throw;
        }
    }

    /// <summary>
    /// \if KO
    /// <para>UDP 소켓을 닫고 백그라운드 수신 루프를 종료합니다.</para>
    /// \endif
    /// \if EN
    /// <para>Closes the UDP socket and terminates the background receive loop.</para>
    /// \endif
    /// </summary>
    /// <param name="cancellationToken">
    /// \if KO
    /// <para>정리 후 연결 해제 취소 여부를 확인하는 토큰입니다.</para>
    /// \endif
    /// \if EN
    /// <para>A token checked for cancellation after cleanup.</para>
    /// \endif
    /// </param>
    /// <returns>
    /// \if KO
    /// <para>비동기 연결 해제 작업입니다.</para>
    /// \endif
    /// \if EN
    /// <para>A task representing asynchronous disconnection.</para>
    /// \endif
    /// </returns>
    public async Task DisconnectAsync(CancellationToken cancellationToken = default)
    {
        if (State == ConnectionState.Disconnected)
        {
            return;
        }

        SetState(ConnectionState.Disconnecting);

        if (_receiveLoopCts is not null)
            await _receiveLoopCts.CancelAsync().ConfigureAwait(false);

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

        SetState(ConnectionState.Disconnected);
    }

    /// <summary>
    /// \if KO
    /// <para>메시지를 프로토콜 페이로드로 인코딩해 구성된 원격 UDP 엔드포인트로 전송합니다.</para>
    /// \endif
    /// \if EN
    /// <para>Encodes a message as a protocol payload and sends it to the configured remote UDP endpoint.</para>
    /// \endif
    /// </summary>
    /// <param name="message">
    /// \if KO
    /// <para>전송할 메시지입니다.</para>
    /// \endif
    /// \if EN
    /// <para>The message to send.</para>
    /// \endif
    /// </param>
    /// <param name="cancellationToken">
    /// \if KO
    /// <para>데이터그램 송신 취소 요청을 감시하는 토큰입니다.</para>
    /// \endif
    /// \if EN
    /// <para>A token used to observe datagram-send cancellation.</para>
    /// \endif
    /// </param>
    /// <returns>
    /// \if KO
    /// <para>비동기 데이터그램 송신 작업입니다.</para>
    /// \endif
    /// \if EN
    /// <para>A task representing asynchronous datagram transmission.</para>
    /// \endif
    /// </returns>
    /// <exception cref="ArgumentNullException">
    /// \if KO
    /// <para>메시지가 <see langword="null"/>인 경우 발생합니다.</para>
    /// \endif
    /// \if EN
    /// <para>Thrown when the message is <see langword="null"/>.</para>
    /// \endif
    /// </exception>
    /// <exception cref="InvalidOperationException">
    /// \if KO
    /// <para>UDP 전송 계층이 연결되지 않은 경우 발생합니다.</para>
    /// \endif
    /// \if EN
    /// <para>Thrown when UDP transport is not connected.</para>
    /// \endif
    /// </exception>
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
    /// \if KO
    /// <para>UDP 소켓과 수신 루프 리소스를 비동기적으로 해제합니다.</para>
    /// \endif
    /// \if EN
    /// <para>Asynchronously releases the UDP socket and receive-loop resources.</para>
    /// \endif
    /// <returns>
    /// \if KO
    /// <para>비동기 리소스 해제 작업입니다.</para>
    /// \endif
    /// \if EN
    /// <para>A value task representing asynchronous disposal.</para>
    /// \endif
    /// </returns>
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        await DisconnectAsync(CancellationToken.None).ConfigureAwait(false);
    }

    /// <summary>
    /// \if KO
    /// <para>데이터그램을 계속 수신해 메시지로 디코딩하고 수신 이벤트를 발생시킵니다.</para>
    /// \endif
    /// \if EN
    /// <para>Continuously receives datagrams, decodes messages, and raises receive events.</para>
    /// \endif
    /// </summary>
    /// <param name="cancellationToken">
    /// \if KO
    /// <para>수신 루프 종료 토큰입니다.</para>
    /// \endif
    /// \if EN
    /// <para>A token used to terminate the receive loop.</para>
    /// \endif
    /// </param>
    /// <returns>
    /// \if KO
    /// <para>백그라운드 수신 루프 작업입니다.</para>
    /// \endif
    /// \if EN
    /// <para>A task representing the background receive loop.</para>
    /// \endif
    /// </returns>
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
                SetState(ConnectionState.Faulted);
            }
        }
        catch
        {
            if (!cancellationToken.IsCancellationRequested)
            {
                SetState(ConnectionState.Faulted);
            }
        }
    }

    /// <summary>
    /// \if KO
    /// <para>UDP 호스트, 포트 및 버퍼 설정을 검증합니다.</para>
    /// \endif
    /// \if EN
    /// <para>Validates UDP host, port, and buffer options.</para>
    /// \endif
    /// </summary>
    /// <param name="options">
    /// \if KO
    /// <para>검증할 UDP 설정입니다.</para>
    /// \endif
    /// \if EN
    /// <para>The UDP options to validate.</para>
    /// \endif
    /// </param>
    /// <exception cref="ArgumentOutOfRangeException">
    /// \if KO
    /// <para>수신 또는 송신 버퍼 크기가 0 이하이거나 로컬·원격 포트가 허용 범위를 벗어난 경우 발생합니다.</para>
    /// \endif
    /// \if EN
    /// <para>Thrown when a receive or send buffer size is nonpositive, or a local or remote port is outside the allowed range.</para>
    /// \endif
    /// </exception>
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

    /// <summary>
    /// \if KO
    /// <para>UDP 포트가 유효한 사용자 포트 범위인지 검증합니다.</para>
    /// \endif
    /// \if EN
    /// <para>Validates that a UDP port is within the valid user-port range.</para>
    /// \endif
    /// </summary>
    /// <param name="port">
    /// \if KO
    /// <para>검증할 포트 번호입니다.</para>
    /// \endif
    /// \if EN
    /// <para>The port number to validate.</para>
    /// \endif
    /// </param>
    /// <param name="parameterName">
    /// \if KO
    /// <para>예외에 사용할 설정 이름입니다.</para>
    /// \endif
    /// \if EN
    /// <para>The option name used in the exception.</para>
    /// \endif
    /// </param>
    /// <exception cref="ArgumentOutOfRangeException">
    /// \if KO
    /// <para>포트가 1~65535 범위를 벗어난 경우 발생합니다.</para>
    /// \endif
    /// \if EN
    /// <para>Thrown when the port is outside the range 1 through 65535.</para>
    /// \endif
    /// </exception>
    private static void ValidatePort(int port, string parameterName)
    {
        if (port <= 0 || port > 65535)
        {
            throw new ArgumentOutOfRangeException(parameterName);
        }
    }

    /// <summary>
    /// \if KO
    /// <para>원자적 연산으로 현재 UDP 상태를 설정합니다.</para>
    /// \endif
    /// \if EN
    /// <para>Sets the current UDP state using an atomic operation.</para>
    /// \endif
    /// </summary>
    /// <param name="state">
    /// \if KO
    /// <para>저장할 새 상태입니다.</para>
    /// \endif
    /// \if EN
    /// <para>The new state to store.</para>
    /// \endif
    /// </param>
    private void SetState(ConnectionState state)
    {
        Interlocked.Exchange(ref _state, (int)state);
    }
}
