namespace Dreamine.Communication.Sockets.Options;

/// <summary>
/// \brief TCP 서버 전송 계층 설정입니다.
/// </summary>
public sealed class TcpServerTransportOptions
{
    /// <summary>
    /// \brief 서버가 바인딩할 IP 주소입니다.
    /// 기본값은 모든 네트워크 인터페이스입니다.
    /// </summary>
    public string Host { get; set; } = "0.0.0.0";

    /// <summary>
    /// \brief 서버가 수신 대기할 포트 번호입니다.
    /// </summary>
    public int Port { get; set; } = 5000;

    /// <summary>
    /// \brief TCP Listener backlog 값입니다.
    /// </summary>
    public int Backlog { get; set; } = 100;

    /// <summary>
    /// \brief 수신 버퍼 크기입니다.
    /// </summary>
    public int ReceiveBufferSize { get; set; } = 8192;

    /// <summary>
    /// \brief 송신 버퍼 크기입니다.
    /// </summary>
    public int SendBufferSize { get; set; } = 8192;
}