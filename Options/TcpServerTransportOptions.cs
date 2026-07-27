using Dreamine.Communication.Sockets.Enums;

namespace Dreamine.Communication.Sockets.Options;

/// <summary>
/// \if KO
/// <para>TCP 서버의 수신 주소, 대기열, 버퍼 및 기본 송신 대상을 구성합니다.</para>
/// \endif
/// \if EN
/// <para>Configures the listen address, backlog, buffers, and default send targets for a TCP server.</para>
/// \endif
/// </summary>
public sealed class TcpServerTransportOptions
{
    /// <summary>
    /// \if KO
    /// <para>서버가 바인딩할 IP 주소를 가져오거나 설정합니다. 기본값은 모든 인터페이스입니다.</para>
    /// \endif
    /// \if EN
    /// <para>Gets or sets the IP address to bind; the default is all interfaces.</para>
    /// \endif
    /// </summary>
    public string Host { get; set; } = "0.0.0.0";

    /// <summary>
    /// \if KO
    /// <para>서버가 수신 대기할 포트를 가져오거나 설정합니다.</para>
    /// \endif
    /// \if EN
    /// <para>Gets or sets the server listen port.</para>
    /// \endif
    /// </summary>
    public int Port { get; set; } = 5000;

    /// <summary>
    /// \if KO
    /// <para>보류 연결 대기열의 최대 크기를 가져오거나 설정합니다.</para>
    /// \endif
    /// \if EN
    /// <para>Gets or sets the maximum pending-connection backlog.</para>
    /// \endif
    /// </summary>
    public int Backlog { get; set; } = 100;

    /// <summary>
    /// \if KO
    /// <para>클라이언트 수신 버퍼 크기(바이트)를 가져오거나 설정합니다.</para>
    /// \endif
    /// \if EN
    /// <para>Gets or sets the client receive buffer size in bytes.</para>
    /// \endif
    /// </summary>
    public int ReceiveBufferSize { get; set; } = 8192;

    /// <summary>
    /// \if KO
    /// <para>클라이언트 송신 버퍼 크기(바이트)를 가져오거나 설정합니다.</para>
    /// \endif
    /// \if EN
    /// <para>Gets or sets the client send buffer size in bytes.</para>
    /// \endif
    /// </summary>
    public int SendBufferSize { get; set; } = 8192;

    /// <summary>
    /// \if KO
    /// <para>기본 메시지 송신 시 사용할 클라이언트 대상 정책을 가져오거나 설정합니다.</para>
    /// \endif
    /// \if EN
    /// <para>Gets or sets the client-target policy used by the default send operation.</para>
    /// \endif
    /// </summary>
    public TcpServerSendTargetMode SendTargetMode { get; set; } = TcpServerSendTargetMode.Broadcast;
}
