namespace Dreamine.Communication.Sockets.Enums;

/// <summary>
/// \if KO
/// <para>TCP 서버 송신 시 메시지를 전달할 연결 클라이언트 선택 정책입니다.</para>
/// \endif
/// \if EN
/// <para>Specifies how connected clients are selected for a TCP server send operation.</para>
/// \endif
/// </summary>
public enum TcpServerSendTargetMode
{
    /// <summary>
    /// \if KO
    /// <para>연결된 모든 클라이언트에게 전송합니다.</para>
    /// \endif
    /// \if EN
    /// <para>Sends to every connected client.</para>
    /// \endif
    /// </summary>
    Broadcast = 0,

    /// <summary>
    /// \if KO
    /// <para>가장 먼저 연결된 클라이언트에게만 전송합니다.</para>
    /// \endif
    /// \if EN
    /// <para>Sends only to the earliest connected client.</para>
    /// \endif
    /// </summary>
    FirstClient = 1,

    /// <summary>
    /// \if KO
    /// <para>가장 최근 연결된 클라이언트에게만 전송합니다.</para>
    /// \endif
    /// \if EN
    /// <para>Sends only to the most recently connected client.</para>
    /// \endif
    /// </summary>
    LastClient = 2
}
