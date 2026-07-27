using System.IO;
using System.Net.Sockets;
using Dreamine.Communication.Abstractions.Enums;
using Dreamine.Communication.Abstractions.Interfaces;
using Dreamine.Communication.Abstractions.Models;
using Dreamine.Communication.Core.Framing;
using Dreamine.Communication.Core.Protocols;
using Dreamine.Communication.Sockets.Options;

namespace Dreamine.Communication.Sockets.Clients;

/// <summary>
/// \if KO
/// <para>TCP 클라이언트 연결에서 구성 가능한 프레임과 프로토콜로 메시지를 송수신합니다.</para>
/// \endif
/// \if EN
/// <para>Sends and receives framed protocol messages over a TCP client connection.</para>
/// \endif
/// </summary>
public sealed class TcpClientTransport : IMessageTransport
{
    /// <summary>
    /// \if KO
    /// <para>options 값을 보관합니다.</para>
    /// \endif
    /// \if EN
    /// <para>Stores the options value.</para>
    /// \endif
    /// </summary>
    private readonly TcpClientTransportOptions _options;
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
    /// <para>frame Codec 값을 보관합니다.</para>
    /// \endif
    /// \if EN
    /// <para>Stores the frame codec value.</para>
    /// \endif
    /// </summary>
    private readonly IMessageFrameCodec _frameCodec;

    /// <summary>
    /// \if KO
    /// <para>client 값을 보관합니다.</para>
    /// \endif
    /// \if EN
    /// <para>Stores the client value.</para>
    /// \endif
    /// </summary>
    private TcpClient? _client;
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
    /// <para>기본 Dreamine JSON 프로토콜과 길이 접두사 프레임으로 TCP 클라이언트를 초기화합니다.</para>
    /// \endif
    /// \if EN
    /// <para>Initializes the TCP client with the default Dreamine JSON protocol and length-prefixed framing.</para>
    /// \endif
    /// </summary>
    /// <param name="options">
    /// \if KO
    /// <para>서버 주소, 버퍼 및 연결 제한 시간 설정입니다.</para>
    /// \endif
    /// \if EN
    /// <para>The server address, buffer, and connection-timeout options.</para>
    /// \endif
    /// </param>
    public TcpClientTransport(TcpClientTransportOptions options)
        : this(
            options,
            new DreamineEnvelopeProtocolAdapter(),
            new LengthPrefixedMessageFrameCodec())
    {
    }

    /// <summary>
    /// \if KO
    /// <para>TCP 설정과 사용자 지정 프로토콜 및 프레임 코덱으로 클라이언트를 초기화합니다.</para>
    /// \endif
    /// \if EN
    /// <para>Initializes the client with TCP options and custom protocol and frame codecs.</para>
    /// \endif
    /// </summary>
    /// <param name="options">
    /// \if KO
    /// <para>서버 주소, 버퍼 및 연결 제한 시간 설정입니다.</para>
    /// \endif
    /// \if EN
    /// <para>The server address, buffer, and connection-timeout options.</para>
    /// \endif
    /// </param>
    /// <param name="protocolAdapter">
    /// \if KO
    /// <para>메시지와 외부 페이로드를 변환할 어댑터입니다.</para>
    /// \endif
    /// \if EN
    /// <para>The adapter that converts messages and external payloads.</para>
    /// \endif
    /// </param>
    /// <param name="frameCodec">
    /// \if KO
    /// <para>TCP 스트림의 메시지 경계를 처리할 코덱입니다.</para>
    /// \endif
    /// \if EN
    /// <para>The codec that handles message boundaries in the TCP stream.</para>
    /// \endif
    /// </param>
    /// <exception cref="ArgumentNullException">
    /// \if KO
    /// <para><paramref name="options"/>, <paramref name="protocolAdapter"/> 또는 <paramref name="frameCodec"/>이 <see langword="null"/>인 경우 발생합니다.</para>
    /// \endif
    /// \if EN
    /// <para>Thrown when <paramref name="options"/>, <paramref name="protocolAdapter"/>, or <paramref name="frameCodec"/> is <see langword="null"/>.</para>
    /// \endif
    /// </exception>
    public TcpClientTransport(
        TcpClientTransportOptions options,
        IMessageProtocolAdapter protocolAdapter,
        IMessageFrameCodec frameCodec)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _protocolAdapter = protocolAdapter ?? throw new ArgumentNullException(nameof(protocolAdapter));
        _frameCodec = frameCodec ?? throw new ArgumentNullException(nameof(frameCodec));

        ValidateOptions(_options);
    }

    /// <summary>
    /// \if KO
    /// <para>스레드 안전하게 현재 TCP 연결 상태를 가져옵니다.</para>
    /// \endif
    /// \if EN
    /// <para>Gets the current TCP connection state in a thread-safe manner.</para>
    /// \endif
    /// </summary>
    public ConnectionState State => (ConnectionState)Volatile.Read(ref _state);

    /// <summary>
    /// \if KO
    /// <para>TCP 전송 방식을 가져옵니다.</para>
    /// \endif
    /// \if EN
    /// <para>Gets the TCP transport kind.</para>
    /// \endif
    /// </summary>
    public TransportKind Kind => TransportKind.Tcp;

    /// <summary>
    /// \if KO
    /// <para>완전한 TCP 프레임을 메시지로 디코딩했을 때 발생합니다.</para>
    /// \endif
    /// \if EN
    /// <para>Occurs when a complete TCP frame has been decoded into a message.</para>
    /// \endif
    /// </summary>
    public event EventHandler<MessageEnvelope>? MessageReceived;

    /// <summary>
    /// \if KO
    /// <para>제한 시간 내에 TCP 서버에 연결하고 백그라운드 수신 루프를 시작합니다.</para>
    /// \endif
    /// \if EN
    /// <para>Connects to the TCP server within the timeout and starts the background receive loop.</para>
    /// \endif
    /// </summary>
    /// <param name="cancellationToken">
    /// \if KO
    /// <para>연결 취소 요청을 감시하는 토큰입니다.</para>
    /// \endif
    /// \if EN
    /// <para>A token used to observe connection cancellation.</para>
    /// \endif
    /// </param>
    /// <returns>
    /// \if KO
    /// <para>비동기 TCP 연결 작업입니다.</para>
    /// \endif
    /// \if EN
    /// <para>A task representing the asynchronous TCP connection.</para>
    /// \endif
    /// </returns>
    /// <exception cref="OperationCanceledException">
    /// \if KO
    /// <para>사용자 취소 또는 연결 제한 시간 만료 시 발생할 수 있습니다.</para>
    /// \endif
    /// \if EN
    /// <para>May be thrown on user cancellation or connection timeout.</para>
    /// \endif
    /// </exception>
    public async Task ConnectAsync(CancellationToken cancellationToken = default)
    {
        if (State is ConnectionState.Connected or ConnectionState.Connecting)
        {
            return;
        }

        SetState(ConnectionState.Connecting);

        try
        {
            CleanupClient();

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

            SetState(ConnectionState.Connected);

            _receiveLoopCts = new CancellationTokenSource();
            _receiveLoopTask = Task.Run(
                () => ReceiveLoopAsync(_receiveLoopCts.Token),
                _receiveLoopCts.Token);
        }
        catch
        {
            SetState(ConnectionState.Faulted);
            CleanupClient();

            throw;
        }
    }

    /// <summary>
    /// \if KO
    /// <para>수신 루프를 중지하고 TCP 클라이언트 연결을 닫습니다.</para>
    /// \endif
    /// \if EN
    /// <para>Stops the receive loop and closes the TCP client connection.</para>
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

        CleanupClient();

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
            catch (IOException)
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
    /// <para>메시지를 외부 프로토콜과 프레임 형식으로 인코딩해 TCP 서버로 전송합니다.</para>
    /// \endif
    /// \if EN
    /// <para>Encodes a message using the external protocol and frame format and sends it to the TCP server.</para>
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
    /// <para>프레임 쓰기 취소 요청을 감시하는 토큰입니다.</para>
    /// \endif
    /// \if EN
    /// <para>A token used to observe frame-write cancellation.</para>
    /// \endif
    /// </param>
    /// <returns>
    /// \if KO
    /// <para>비동기 메시지 전송 작업입니다.</para>
    /// \endif
    /// \if EN
    /// <para>A task representing asynchronous message transmission.</para>
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
    /// <para>TCP 클라이언트가 연결되지 않은 경우 발생합니다.</para>
    /// \endif
    /// \if EN
    /// <para>Thrown when the TCP client is not connected.</para>
    /// \endif
    /// </exception>
    public async Task SendAsync(
        MessageEnvelope message,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(message);

        if (_client is null || State != ConnectionState.Connected)
        {
            throw new InvalidOperationException("TCP client is not connected.");
        }

        try
        {
            var stream = _client.GetStream();
            var payload = _protocolAdapter.Encode(message);

            await _frameCodec.WriteFrameAsync(stream, payload, cancellationToken)
                .ConfigureAwait(false);
        }
        catch
        {
            SetState(ConnectionState.Faulted);
            CleanupClient();

            throw;
        }
    }

    /// <summary>
    /// \if KO
    /// <para>TCP 연결과 수신 루프 리소스를 비동기적으로 해제합니다.</para>
    /// \endif
    /// \if EN
    /// <para>Asynchronously releases the TCP connection and receive-loop resources.</para>
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
    /// <para>TCP 스트림에서 프레임을 계속 읽어 메시지로 디코딩하고 수신 이벤트를 발생시킵니다.</para>
    /// \endif
    /// \if EN
    /// <para>Continuously reads TCP frames, decodes messages, and raises receive events.</para>
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

                var message = _protocolAdapter.Decode(payload);
                MessageReceived?.Invoke(this, message);
            }

            if (!cancellationToken.IsCancellationRequested &&
                State == ConnectionState.Connected)
            {
                SetState(ConnectionState.Disconnected);
                CleanupClient();
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
                CleanupClient();
            }
        }
        catch (IOException)
        {
            if (!cancellationToken.IsCancellationRequested)
            {
                SetState(ConnectionState.Faulted);
                CleanupClient();
            }
        }
        catch
        {
            if (!cancellationToken.IsCancellationRequested)
            {
                SetState(ConnectionState.Faulted);
                CleanupClient();
            }
        }
    }

    /// <summary>
    /// \if KO
    /// <para>정리 예외를 전파하지 않고 TCP 클라이언트를 닫고 해제합니다.</para>
    /// \endif
    /// \if EN
    /// <para>Closes and disposes the TCP client without propagating cleanup exceptions.</para>
    /// \endif
    /// </summary>
    private void CleanupClient()
    {
        try
        {
            _client?.Close();
        }
        catch
        {
        }

        try
        {
            _client?.Dispose();
        }
        catch
        {
        }

        _client = null;
    }

    /// <summary>
    /// \if KO
    /// <para>원자적 연산으로 현재 연결 상태를 설정합니다.</para>
    /// \endif
    /// \if EN
    /// <para>Sets the connection state using an atomic operation.</para>
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

    /// <summary>
    /// \if KO
    /// <para>TCP 호스트, 포트, 버퍼 및 연결 제한 시간 설정을 검증합니다.</para>
    /// \endif
    /// \if EN
    /// <para>Validates TCP host, port, buffer, and connection-timeout options.</para>
    /// \endif
    /// </summary>
    /// <param name="options">
    /// \if KO
    /// <para>검증할 TCP 클라이언트 설정입니다.</para>
    /// \endif
    /// \if EN
    /// <para>The TCP client options to validate.</para>
    /// \endif
    /// </param>
    /// <exception cref="ArgumentException">
    /// \if KO
    /// <para>호스트가 비어 있는 경우 발생합니다.</para>
    /// \endif
    /// \if EN
    /// <para>Thrown when the host is empty.</para>
    /// \endif
    /// </exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// \if KO
    /// <para>수치 설정이 허용 범위를 벗어난 경우 발생합니다.</para>
    /// \endif
    /// \if EN
    /// <para>Thrown when a numeric option is outside its allowed range.</para>
    /// \endif
    /// </exception>
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
