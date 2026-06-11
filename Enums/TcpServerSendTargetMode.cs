namespace Dreamine.Communication.Sockets.Enums;

/// <summary>
/// TCP 서버에서 SendAsync 호출 시 메시지를 전달할 클라이언트 대상 정책입니다.
/// </summary>
public enum TcpServerSendTargetMode
{
    /// <summary>
    /// 연결된 모든 클라이언트에게 메시지를 전송합니다.
    /// </summary>
    Broadcast = 0,

    /// <summary>
    /// 가장 먼저 연결된 클라이언트에게만 메시지를 전송합니다.
    /// </summary>
    FirstClient = 1,

    /// <summary>
    /// 가장 마지막에 연결된 클라이언트에게만 메시지를 전송합니다.
    /// </summary>
    LastClient = 2
}
