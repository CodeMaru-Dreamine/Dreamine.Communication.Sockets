using System.Net;

namespace Dreamine.Communication.Sockets.Options;

/// <summary>
/// \if KO
/// <para>UDP 소켓의 로컬·원격 엔드포인트, 버퍼 및 소켓 동작을 구성합니다.</para>
/// \endif
/// \if EN
/// <para>Configures local and remote endpoints, buffers, and socket behavior for UDP transport.</para>
/// \endif
/// </summary>
public sealed class UdpTransportOptions
{
    /// <summary>
    /// \if KO
    /// <para>UDP 소켓이 바인딩할 로컬 호스트를 가져오거나 설정합니다.</para>
    /// \endif
    /// \if EN
    /// <para>Gets or sets the local host to which the UDP socket binds.</para>
    /// \endif
    /// </summary>
    public string LocalHost { get; set; } = "127.0.0.1";

    /// <summary>
    /// \if KO
    /// <para>UDP 소켓이 바인딩할 로컬 포트를 가져오거나 설정합니다.</para>
    /// \endif
    /// \if EN
    /// <para>Gets or sets the local UDP port.</para>
    /// \endif
    /// </summary>
    public int LocalPort { get; set; } = 16001;

    /// <summary>
    /// \if KO
    /// <para>UDP 메시지를 송신할 원격 호스트를 가져오거나 설정합니다.</para>
    /// \endif
    /// \if EN
    /// <para>Gets or sets the remote host for UDP messages.</para>
    /// \endif
    /// </summary>
    public string RemoteHost { get; set; } = "127.0.0.1";

    /// <summary>
    /// \if KO
    /// <para>UDP 메시지를 송신할 원격 포트를 가져오거나 설정합니다.</para>
    /// \endif
    /// \if EN
    /// <para>Gets or sets the remote UDP port.</para>
    /// \endif
    /// </summary>
    public int RemotePort { get; set; } = 16002;

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
    /// <para>브로드캐스트 송신을 허용할지 여부를 가져오거나 설정합니다.</para>
    /// \endif
    /// \if EN
    /// <para>Gets or sets whether broadcast transmission is enabled.</para>
    /// \endif
    /// </summary>
    public bool EnableBroadcast { get; set; }

    /// <summary>
    /// \if KO
    /// <para>로컬 주소와 포트 재사용을 허용할지 여부를 가져오거나 설정합니다.</para>
    /// \endif
    /// \if EN
    /// <para>Gets or sets whether reuse of the local address and port is enabled.</para>
    /// \endif
    /// </summary>
    public bool ReuseAddress { get; set; }

    /// <summary>
    /// \if KO
    /// <para>구성된 로컬 호스트와 포트에서 바인딩 엔드포인트를 생성합니다.</para>
    /// \endif
    /// \if EN
    /// <para>Creates a bind endpoint from the configured local host and port.</para>
    /// \endif
    /// </summary>
    /// <returns>
    /// \if KO
    /// <para>로컬 UDP 엔드포인트입니다.</para>
    /// \endif
    /// \if EN
    /// <para>The local UDP endpoint.</para>
    /// \endif
    /// </returns>
    public IPEndPoint CreateLocalEndPoint()
    {
        return new IPEndPoint(ParseHost(LocalHost, allowAny: true), LocalPort);
    }

    /// <summary>
    /// \if KO
    /// <para>구성된 원격 호스트와 포트에서 송신 엔드포인트를 생성합니다.</para>
    /// \endif
    /// \if EN
    /// <para>Creates a send endpoint from the configured remote host and port.</para>
    /// \endif
    /// </summary>
    /// <returns>
    /// \if KO
    /// <para>원격 UDP 엔드포인트입니다.</para>
    /// \endif
    /// \if EN
    /// <para>The remote UDP endpoint.</para>
    /// \endif
    /// </returns>
    public IPEndPoint CreateRemoteEndPoint()
    {
        return new IPEndPoint(ParseHost(RemoteHost, allowAny: false), RemotePort);
    }

    /// <summary>
    /// \if KO
    /// <para>호스트 문자열을 IP 주소로 변환하고 필요 시 모든 인터페이스 주소를 허용합니다.</para>
    /// \endif
    /// \if EN
    /// <para>Parses a host string as an IP address and optionally permits the all-interfaces address.</para>
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
    /// <param name="allowAny">
    /// \if KO
    /// <para>0.0.0.0을 모든 인터페이스로 허용할지 여부입니다.</para>
    /// \endif
    /// \if EN
    /// <para>Whether 0.0.0.0 is allowed as all interfaces.</para>
    /// \endif
    /// </param>
    /// <returns>
    /// \if KO
    /// <para>변환된 IP 주소입니다.</para>
    /// \endif
    /// \if EN
    /// <para>The parsed IP address.</para>
    /// \endif
    /// </returns>
    /// <exception cref="ArgumentException">
    /// \if KO
    /// <para>호스트가 비어 있거나 올바른 IP 주소가 아닌 경우 발생합니다.</para>
    /// \endif
    /// \if EN
    /// <para>Thrown when the host is empty or not a valid IP address.</para>
    /// \endif
    /// </exception>
    private static IPAddress ParseHost(string host, bool allowAny)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(host);

        if (allowAny && host == "0.0.0.0")
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

        throw new ArgumentException($"Invalid UDP host: {host}", nameof(host));
    }
}
