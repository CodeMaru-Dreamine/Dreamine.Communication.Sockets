namespace Dreamine.Communication.Sockets.Options;

/// <summary>
/// TCP 클라이언트 전송 계층 설정입니다.
/// </summary>
public sealed class TcpClientTransportOptions
{
    /// <summary>
    /// 연결할 서버 호스트입니다.
    /// </summary>
    public string Host { get; set; } = "127.0.0.1";

    /// <summary>
    /// 연결할 서버 포트입니다.
    /// </summary>
    public int Port { get; set; } = 0;

    /// <summary>
    /// 수신 버퍼 크기입니다.
    /// </summary>
    public int ReceiveBufferSize { get; set; } = 8192;

    /// <summary>
    /// 송신 버퍼 크기입니다.
    /// </summary>
    public int SendBufferSize { get; set; } = 8192;

    /// <summary>
    /// 연결 타임아웃(ms)입니다.
    /// </summary>
    public int ConnectTimeoutMs { get; set; } = 5000;
}