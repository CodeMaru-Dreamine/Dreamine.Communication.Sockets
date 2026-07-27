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
/// \if KO
/// <para>여러 TCP 클라이언트의 연결, 수신 및 대상별 메시지 송신을 관리합니다.</para>
/// \endif
/// \if EN
/// <para>Manages connections, receiving, and targeted message sending for multiple TCP clients.</para>
/// \endif
/// </summary>
/// <remarks>
/// \if KO
/// <para>여러 TCP 클라이언트의 연결, 수신 및 대상별 메시지 송신을 관리합니다.</para>
/// \endif
/// \if EN
/// <para>Manages connections, receiving, and targeted message sending for multiple TCP clients.</para>
/// \endif
/// </remarks>
public sealed class TcpServerTransport : IMessageTransport, IServerTransportMonitor
{
    /// <summary>
    /// \if KO
    /// <para>options 값을 보관합니다.</para>
    /// \endif
    /// \if EN
    /// <para>Stores the options value.</para>
    /// \endif
    /// </summary>
    private readonly TcpServerTransportOptions _options;
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
    /// <para>clients 값을 보관합니다.</para>
    /// \endif
    /// \if EN
    /// <para>Stores the clients value.</para>
    /// \endif
    /// </summary>
    private readonly ConcurrentDictionary<Guid, TcpClientConnectionEntry> _clients = new();

    /// <summary>
    /// \if KO
    /// <para>listener 값을 보관합니다.</para>
    /// \endif
    /// \if EN
    /// <para>Stores the listener value.</para>
    /// \endif
    /// </summary>
    private TcpListener? _listener;
    /// <summary>
    /// \if KO
    /// <para>server Cts 값을 보관합니다.</para>
    /// \endif
    /// \if EN
    /// <para>Stores the server cts value.</para>
    /// \endif
    /// </summary>
    private CancellationTokenSource? _serverCts;
    /// <summary>
    /// \if KO
    /// <para>accept Loop Task 값을 보관합니다.</para>
    /// \endif
    /// \if EN
    /// <para>Stores the accept loop task value.</para>
    /// \endif
    /// </summary>
    private Task? _acceptLoopTask;
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
    /// <para>기본 Dreamine JSON 프로토콜과 길이 접두사 프레임으로 TCP 서버를 초기화합니다.</para>
    /// \endif
    /// \if EN
    /// <para>Initializes the TCP server with the default Dreamine JSON protocol and length-prefixed framing.</para>
    /// \endif
    /// </summary>
    /// <param name="options">
    /// \if KO
    /// <para>수신 주소, 대기열, 버퍼 및 송신 대상 설정입니다.</para>
    /// \endif
    /// \if EN
    /// <para>The listen address, backlog, buffer, and send-target options.</para>
    /// \endif
    /// </param>
    public TcpServerTransport(TcpServerTransportOptions options)
        : this(
            options,
            new DreamineEnvelopeProtocolAdapter(),
            new LengthPrefixedMessageFrameCodec())
    {
    }

    /// <summary>
    /// \if KO
    /// <para>TCP 서버 설정과 사용자 지정 프로토콜 및 프레임 코덱으로 서버를 초기화합니다.</para>
    /// \endif
    /// \if EN
    /// <para>Initializes the server with TCP options and custom protocol and frame codecs.</para>
    /// \endif
    /// </summary>
    /// <param name="options">
    /// \if KO
    /// <para>수신 주소, 대기열, 버퍼 및 송신 대상 설정입니다.</para>
    /// \endif
    /// \if EN
    /// <para>The listen address, backlog, buffer, and send-target options.</para>
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
    /// <para>클라이언트 스트림의 메시지 경계를 처리할 코덱입니다.</para>
    /// \endif
    /// \if EN
    /// <para>The codec that handles message boundaries in client streams.</para>
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
    /// \if KO
    /// <para>스레드 안전하게 현재 서버 수신 대기 상태를 가져옵니다.</para>
    /// \endif
    /// \if EN
    /// <para>Gets the current server listen state in a thread-safe manner.</para>
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
    /// <para>현재 서버에 연결된 TCP 클라이언트 수를 가져옵니다.</para>
    /// \endif
    /// \if EN
    /// <para>Gets the number of TCP clients currently connected to the server.</para>
    /// \endif
    /// </summary>
    public int ConnectedClientCount => _clients.Count;

    /// <summary>
    /// \if KO
    /// <para>기본 송신 작업에서 사용할 클라이언트 대상 정책을 가져오거나 설정합니다.</para>
    /// \endif
    /// \if EN
    /// <para>Gets or sets the client-target policy used by the default send operation.</para>
    /// \endif
    /// </summary>
    public TcpServerSendTargetMode SendTargetMode
    {
        get => _options.SendTargetMode;
        set => _options.SendTargetMode = value;
    }

    /// <summary>
    /// \if KO
    /// <para>클라이언트 프레임을 메시지로 디코딩했을 때 발생합니다.</para>
    /// \endif
    /// \if EN
    /// <para>Occurs when a client frame has been decoded into a message.</para>
    /// \endif
    /// </summary>
    public event EventHandler<MessageEnvelope>? MessageReceived;

    /// <summary>
    /// \if KO
    /// <para>서버에 연결된 클라이언트 수가 변경될 때 발생합니다.</para>
    /// \endif
    /// \if EN
    /// <para>Occurs when the connected-client count changes.</para>
    /// \endif
    /// </summary>
    public event EventHandler<int>? ConnectedClientCountChanged;

    /// <summary>
    /// \if KO
    /// <para>TCP Listener를 시작하고 백그라운드 클라이언트 수락 루프를 실행합니다.</para>
    /// \endif
    /// \if EN
    /// <para>Starts the TCP listener and background client-accept loop.</para>
    /// \endif
    /// </summary>
    /// <param name="cancellationToken">
    /// \if KO
    /// <para>서버 및 수락 루프 수명과 연결할 취소 토큰입니다.</para>
    /// \endif
    /// \if EN
    /// <para>A cancellation token linked to server and accept-loop lifetime.</para>
    /// \endif
    /// </param>
    /// <returns>
    /// \if KO
    /// <para>TCP 서버 시작 작업입니다.</para>
    /// \endif
    /// \if EN
    /// <para>A task representing TCP server startup.</para>
    /// \endif
    /// </returns>
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
    /// \if KO
    /// <para>수락 루프와 Listener를 중지하고 연결된 모든 클라이언트를 해제합니다.</para>
    /// \endif
    /// \if EN
    /// <para>Stops the accept loop and listener and releases all connected clients.</para>
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
    /// <para>비동기 서버 종료 작업입니다.</para>
    /// \endif
    /// \if EN
    /// <para>A task representing asynchronous server shutdown.</para>
    /// \endif
    /// </returns>
    public async Task DisconnectAsync(CancellationToken cancellationToken = default)
    {
        if (State == ConnectionState.Disconnected)
        {
            return;
        }

        SetState(ConnectionState.Disconnecting);

        if (_serverCts is not null)
            await _serverCts.CancelAsync().ConfigureAwait(false);

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
    /// \if KO
    /// <para>구성된 기본 대상 정책에 따라 연결된 클라이언트에게 메시지를 전송합니다.</para>
    /// \endif
    /// \if EN
    /// <para>Sends a message to connected clients according to the configured target policy.</para>
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
    /// <para>송신 취소 요청을 감시하는 토큰입니다.</para>
    /// \endif
    /// \if EN
    /// <para>A token used to observe send cancellation.</para>
    /// \endif
    /// </param>
    /// <returns>
    /// \if KO
    /// <para>비동기 대상별 송신 작업입니다.</para>
    /// \endif
    /// \if EN
    /// <para>A task representing asynchronous targeted sending.</para>
    /// \endif
    /// </returns>
    public Task SendAsync(
        MessageEnvelope message,
        CancellationToken cancellationToken = default)
    {
        return SendAsync(SendTargetMode, message, cancellationToken);
    }

    /// <summary>
    /// \if KO
    /// <para>지정한 대상 정책으로 선택한 클라이언트들에게 메시지를 병렬 전송합니다.</para>
    /// \endif
    /// \if EN
    /// <para>Sends a message in parallel to clients selected by the specified target policy.</para>
    /// \endif
    /// </summary>
    /// <param name="targetMode">
    /// \if KO
    /// <para>클라이언트 선택 정책입니다.</para>
    /// \endif
    /// \if EN
    /// <para>The client-selection policy.</para>
    /// \endif
    /// </param>
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
    /// <para>모든 클라이언트 송신 취소 토큰입니다.</para>
    /// \endif
    /// \if EN
    /// <para>A token used to cancel all client sends.</para>
    /// \endif
    /// </param>
    /// <returns>
    /// \if KO
    /// <para>선택된 모든 클라이언트의 병렬 송신 작업입니다.</para>
    /// \endif
    /// \if EN
    /// <para>A task representing parallel sends to all selected clients.</para>
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
    /// <para>서버가 수신 대기 중이 아니거나 대상 클라이언트가 없는 경우 발생합니다.</para>
    /// \endif
    /// \if EN
    /// <para>Thrown when the server is not listening or no target client exists.</para>
    /// \endif
    /// </exception>
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
    /// \if KO
    /// <para>연결된 모든 TCP 클라이언트에게 메시지를 병렬 전송합니다.</para>
    /// \endif
    /// \if EN
    /// <para>Sends a message in parallel to all connected TCP clients.</para>
    /// \endif
    /// </summary>
    /// <param name="message">
    /// \if KO
    /// <para>브로드캐스트할 메시지입니다.</para>
    /// \endif
    /// \if EN
    /// <para>The message to broadcast.</para>
    /// \endif
    /// </param>
    /// <param name="cancellationToken">
    /// \if KO
    /// <para>브로드캐스트 취소 토큰입니다.</para>
    /// \endif
    /// \if EN
    /// <para>A token used to cancel broadcasting.</para>
    /// \endif
    /// </param>
    /// <returns>
    /// \if KO
    /// <para>비동기 브로드캐스트 작업입니다.</para>
    /// \endif
    /// \if EN
    /// <para>A task representing asynchronous broadcasting.</para>
    /// \endif
    /// </returns>
    public Task BroadcastAsync(
        MessageEnvelope message,
        CancellationToken cancellationToken = default)
    {
        return SendAsync(TcpServerSendTargetMode.Broadcast, message, cancellationToken);
    }


    /// <summary>
    /// \if KO
    /// <para>연결 시각과 대상 정책에 따라 송신 대상 클라이언트 스냅샷을 생성합니다.</para>
    /// \endif
    /// \if EN
    /// <para>Creates a target-client snapshot based on connection time and target policy.</para>
    /// \endif
    /// </summary>
    /// <param name="targetMode">
    /// \if KO
    /// <para>적용할 대상 선택 정책입니다.</para>
    /// \endif
    /// \if EN
    /// <para>The target-selection policy to apply.</para>
    /// \endif
    /// </param>
    /// <returns>
    /// \if KO
    /// <para>선택된 클라이언트 연결 배열입니다.</para>
    /// \endif
    /// \if EN
    /// <para>An array of selected client connections.</para>
    /// \endif
    /// </returns>
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
    /// \if KO
    /// <para>Listener, 수락 루프 및 클라이언트 연결을 비동기적으로 해제합니다.</para>
    /// \endif
    /// \if EN
    /// <para>Asynchronously releases the listener, accept loop, and client connections.</para>
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
    /// <para>클라이언트별 잠금을 사용해 한 클라이언트에 프레임을 순차적으로 전송합니다.</para>
    /// \endif
    /// \if EN
    /// <para>Sends a frame sequentially to one client using its per-client lock.</para>
    /// \endif
    /// </summary>
    /// <param name="target">
    /// \if KO
    /// <para>대상 클라이언트 연결 정보입니다.</para>
    /// \endif
    /// \if EN
    /// <para>The target client connection.</para>
    /// \endif
    /// </param>
    /// <param name="payload">
    /// \if KO
    /// <para>프레임으로 전송할 프로토콜 페이로드입니다.</para>
    /// \endif
    /// \if EN
    /// <para>The protocol payload to send as a frame.</para>
    /// \endif
    /// </param>
    /// <param name="cancellationToken">
    /// \if KO
    /// <para>잠금 대기와 송신 취소 토큰입니다.</para>
    /// \endif
    /// \if EN
    /// <para>A token used to cancel lock waiting and sending.</para>
    /// \endif
    /// </param>
    /// <returns>
    /// \if KO
    /// <para>단일 클라이언트 송신 작업입니다.</para>
    /// \endif
    /// \if EN
    /// <para>A task representing the single-client send.</para>
    /// \endif
    /// </returns>
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

    /// <summary>
    /// \if KO
    /// <para>TCP 클라이언트를 계속 수락하고 각 연결의 수신 루프를 시작합니다.</para>
    /// \endif
    /// \if EN
    /// <para>Continuously accepts TCP clients and starts a receive loop for each connection.</para>
    /// \endif
    /// </summary>
    /// <param name="cancellationToken">
    /// \if KO
    /// <para>수락 루프 종료 토큰입니다.</para>
    /// \endif
    /// \if EN
    /// <para>A token used to terminate the accept loop.</para>
    /// \endif
    /// </param>
    /// <returns>
    /// \if KO
    /// <para>백그라운드 수락 루프 작업입니다.</para>
    /// \endif
    /// \if EN
    /// <para>A task representing the background accept loop.</para>
    /// \endif
    /// </returns>
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

    /// <summary>
    /// \if KO
    /// <para>특정 클라이언트의 프레임을 계속 읽어 메시지 수신 이벤트를 발생시킵니다.</para>
    /// \endif
    /// \if EN
    /// <para>Continuously reads frames from one client and raises message-received events.</para>
    /// \endif
    /// </summary>
    /// <param name="clientId">
    /// \if KO
    /// <para>클라이언트 연결 식별자입니다.</para>
    /// \endif
    /// \if EN
    /// <para>The client connection identifier.</para>
    /// \endif
    /// </param>
    /// <param name="client">
    /// \if KO
    /// <para>데이터를 읽을 TCP 클라이언트입니다.</para>
    /// \endif
    /// \if EN
    /// <para>The TCP client to read.</para>
    /// \endif
    /// </param>
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
    /// <para>클라이언트 수신 루프 작업입니다.</para>
    /// \endif
    /// \if EN
    /// <para>A task representing the client receive loop.</para>
    /// \endif
    /// </returns>
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

    /// <summary>
    /// \if KO
    /// <para>정리 예외를 전파하지 않고 TCP Listener를 중지합니다.</para>
    /// \endif
    /// \if EN
    /// <para>Stops the TCP listener without propagating cleanup exceptions.</para>
    /// \endif
    /// </summary>
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

    /// <summary>
    /// \if KO
    /// <para>현재 연결된 모든 클라이언트를 제거하고 해제합니다.</para>
    /// \endif
    /// \if EN
    /// <para>Removes and disposes all currently connected clients.</para>
    /// \endif
    /// </summary>
    private void CleanupClients()
    {
        foreach (var pair in _clients.ToArray())
        {
            RemoveClient(pair.Key, pair.Value.Client);
        }
    }

    /// <summary>
    /// \if KO
    /// <para>연결 사전에서 클라이언트를 제거하고 소켓을 닫은 뒤 개수 변경을 알립니다.</para>
    /// \endif
    /// \if EN
    /// <para>Removes a client from the connection map, closes its socket, and reports the count change.</para>
    /// \endif
    /// </summary>
    /// <param name="clientId">
    /// \if KO
    /// <para>제거할 연결 식별자입니다.</para>
    /// \endif
    /// \if EN
    /// <para>The connection identifier to remove.</para>
    /// \endif
    /// </param>
    /// <param name="client">
    /// \if KO
    /// <para>닫고 해제할 TCP 클라이언트입니다.</para>
    /// \endif
    /// \if EN
    /// <para>The TCP client to close and dispose.</para>
    /// \endif
    /// </param>
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

    /// <summary>
    /// \if KO
    /// <para>원자적 연산으로 현재 서버 상태를 설정합니다.</para>
    /// \endif
    /// \if EN
    /// <para>Sets the current server state using an atomic operation.</para>
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
    /// <para>현재 연결 클라이언트 수로 변경 이벤트를 발생시킵니다.</para>
    /// \endif
    /// \if EN
    /// <para>Raises the count-change event with the current connected-client count.</para>
    /// \endif
    /// </summary>
    private void NotifyConnectedClientCountChanged()
    {
        ConnectedClientCountChanged?.Invoke(this, ConnectedClientCount);
    }

    /// <summary>
    /// \if KO
    /// <para>서버 바인딩 호스트 문자열을 IPv4 주소로 변환합니다.</para>
    /// \endif
    /// \if EN
    /// <para>Parses a server bind-host string into an IPv4 address.</para>
    /// \endif
    /// </summary>
    /// <param name="host">
    /// \if KO
    /// <para>변환할 호스트 문자열입니다.</para>
    /// \endif
    /// \if EN
    /// <para>The host string to parse.</para>
    /// \endif
    /// </param>
    /// <returns>
    /// \if KO
    /// <para>바인딩에 사용할 IP 주소입니다.</para>
    /// \endif
    /// \if EN
    /// <para>The IP address to use for binding.</para>
    /// \endif
    /// </returns>
    /// <exception cref="ArgumentException">
    /// \if KO
    /// <para>올바른 IP 주소가 아닌 경우 발생합니다.</para>
    /// \endif
    /// \if EN
    /// <para>Thrown when the host is not a valid IP address.</para>
    /// \endif
    /// </exception>
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

    /// <summary>
    /// \if KO
    /// <para>TCP 서버 호스트, 포트, 대기열 및 버퍼 설정을 검증합니다.</para>
    /// \endif
    /// \if EN
    /// <para>Validates TCP server host, port, backlog, and buffer options.</para>
    /// \endif
    /// </summary>
    /// <param name="options">
    /// \if KO
    /// <para>검증할 TCP 서버 설정입니다.</para>
    /// \endif
    /// \if EN
    /// <para>The TCP server options to validate.</para>
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

    /// <summary>
    /// \if KO
    /// <para>서버가 수락한 TCP 클라이언트의 식별자, 소켓, 연결 시각 및 송신 동기화를 보관합니다.</para>
    /// \endif
    /// \if EN
    /// <para>Stores the identifier, socket, connection time, and send synchronization for a TCP client accepted by the server.</para>
    /// \endif
    /// </summary>
    private sealed class TcpClientConnectionEntry
    {
        /// <summary>
        /// \if KO
        /// <para>클라이언트 식별자, 소켓 및 연결 시각으로 연결 정보를 초기화합니다.</para>
        /// \endif
        /// \if EN
        /// <para>Initializes connection information with a client identifier, socket, and connection time.</para>
        /// \endif
        /// </summary>
        /// <param name="clientId">
        /// \if KO
        /// <para>연결 식별자입니다.</para>
        /// \endif
        /// \if EN
        /// <para>The connection identifier.</para>
        /// \endif
        /// </param>
        /// <param name="client">
        /// \if KO
        /// <para>연결된 TCP 클라이언트입니다.</para>
        /// \endif
        /// \if EN
        /// <para>The connected TCP client.</para>
        /// \endif
        /// </param>
        /// <param name="connectedAt">
        /// \if KO
        /// <para>연결이 수락된 시각입니다.</para>
        /// \endif
        /// \if EN
        /// <para>The time at which the connection was accepted.</para>
        /// \endif
        /// </param>
        /// <exception cref="ArgumentNullException">
        /// \if KO
        /// <para><paramref name="client"/>가 <see langword="null"/>인 경우 발생합니다.</para>
        /// \endif
        /// \if EN
        /// <para>Thrown when <paramref name="client"/> is <see langword="null"/>.</para>
        /// \endif
        /// </exception>
        public TcpClientConnectionEntry(
            Guid clientId,
            TcpClient client,
            DateTimeOffset connectedAt)
        {
            ClientId = clientId;
            Client = client ?? throw new ArgumentNullException(nameof(client));
            ConnectedAt = connectedAt;
        }

        /// <summary>
        /// \if KO
        /// <para>클라이언트 연결 식별자를 가져옵니다.</para>
        /// \endif
        /// \if EN
        /// <para>Gets the client connection identifier.</para>
        /// \endif
        /// </summary>
        public Guid ClientId { get; }

        /// <summary>
        /// \if KO
        /// <para>연결된 TCP 클라이언트를 가져옵니다.</para>
        /// \endif
        /// \if EN
        /// <para>Gets the connected TCP client.</para>
        /// \endif
        /// </summary>
        public TcpClient Client { get; }

        /// <summary>
        /// \if KO
        /// <para>연결이 수락된 시각을 가져옵니다.</para>
        /// \endif
        /// \if EN
        /// <para>Gets the time at which the connection was accepted.</para>
        /// \endif
        /// </summary>
        public DateTimeOffset ConnectedAt { get; }

        /// <summary>
        /// \if KO
        /// <para>이 클라이언트에 대한 프레임 쓰기를 직렬화하는 잠금을 가져옵니다.</para>
        /// \endif
        /// \if EN
        /// <para>Gets the lock that serializes frame writes to this client.</para>
        /// \endif
        /// </summary>
        public SemaphoreSlim SendLock { get; } = new(1, 1);
    }

}
