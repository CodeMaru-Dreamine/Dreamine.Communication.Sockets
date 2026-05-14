using System.Net;

namespace Dreamine.Communication.Sockets.Options;

/// <summary>
/// \brief UDP 전송 계층 설정입니다.
/// </summary>
public sealed class UdpTransportOptions
{
    /// <summary>
    /// \brief UDP 소켓이 바인딩할 로컬 호스트입니다.
    /// </summary>
    public string LocalHost { get; set; } = "127.0.0.1";

    /// <summary>
    /// \brief UDP 소켓이 바인딩할 로컬 포트입니다.
    /// </summary>
    public int LocalPort { get; set; } = 16001;

    /// <summary>
    /// \brief UDP 메시지를 송신할 원격 호스트입니다.
    /// </summary>
    public string RemoteHost { get; set; } = "127.0.0.1";

    /// <summary>
    /// \brief UDP 메시지를 송신할 원격 포트입니다.
    /// </summary>
    public int RemotePort { get; set; } = 16002;

    /// <summary>
    /// \brief 수신 버퍼 크기입니다.
    /// </summary>
    public int ReceiveBufferSize { get; set; } = 8192;

    /// <summary>
    /// \brief 송신 버퍼 크기입니다.
    /// </summary>
    public int SendBufferSize { get; set; } = 8192;

    /// <summary>
    /// \brief 브로드캐스트 송신을 허용할지 여부입니다.
    /// </summary>
    public bool EnableBroadcast { get; set; }

    /// <summary>
    /// \brief 같은 로컬 주소/포트의 재사용을 허용할지 여부입니다.
    /// </summary>
    public bool ReuseAddress { get; set; }

    /// <summary>
    /// \brief 로컬 엔드포인트를 생성합니다.
    /// </summary>
    /// <returns>로컬 UDP 엔드포인트입니다.</returns>
    public IPEndPoint CreateLocalEndPoint()
    {
        return new IPEndPoint(ParseHost(LocalHost, allowAny: true), LocalPort);
    }

    /// <summary>
    /// \brief 원격 엔드포인트를 생성합니다.
    /// </summary>
    /// <returns>원격 UDP 엔드포인트입니다.</returns>
    public IPEndPoint CreateRemoteEndPoint()
    {
        return new IPEndPoint(ParseHost(RemoteHost, allowAny: false), RemotePort);
    }

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
