namespace Dreamine.Communication.Sockets.Options;

/// <summary>
/// \if KO
/// <para>TCP 클라이언트의 서버 주소, 버퍼 및 연결 제한 시간을 구성합니다.</para>
/// \endif
/// \if EN
/// <para>Configures the server address, buffers, and connection timeout for a TCP client.</para>
/// \endif
/// </summary>
public sealed class TcpClientTransportOptions
{
    /// <summary>
    /// \if KO
    /// <para>연결할 서버 호스트를 가져오거나 설정합니다.</para>
    /// \endif
    /// \if EN
    /// <para>Gets or sets the server host to connect to.</para>
    /// \endif
    /// </summary>
    public string Host { get; set; } = "127.0.0.1";

    /// <summary>
    /// \if KO
    /// <para>연결할 서버 포트를 가져오거나 설정합니다.</para>
    /// \endif
    /// \if EN
    /// <para>Gets or sets the server port to connect to.</para>
    /// \endif
    /// </summary>
    public int Port { get; set; } = 0;

    /// <summary>
    /// \if KO
    /// <para>수신 버퍼 크기(바이트)를 가져오거나 설정합니다.</para>
    /// \endif
    /// \if EN
    /// <para>Gets or sets the receive buffer size in bytes.</para>
    /// \endif
    /// </summary>
    public int ReceiveBufferSize { get; set; } = 8192;

    /// <summary>
    /// \if KO
    /// <para>송신 버퍼 크기(바이트)를 가져오거나 설정합니다.</para>
    /// \endif
    /// \if EN
    /// <para>Gets or sets the send buffer size in bytes.</para>
    /// \endif
    /// </summary>
    public int SendBufferSize { get; set; } = 8192;

    /// <summary>
    /// \if KO
    /// <para>연결 제한 시간(밀리초)을 가져오거나 설정합니다.</para>
    /// \endif
    /// \if EN
    /// <para>Gets or sets the connection timeout in milliseconds.</para>
    /// \endif
    /// </summary>
    public int ConnectTimeoutMs { get; set; } = 5000;
}
