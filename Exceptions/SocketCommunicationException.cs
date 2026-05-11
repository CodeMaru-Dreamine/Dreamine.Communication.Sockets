using System;
using Dreamine.Communication.Abstractions.Exceptions;

namespace Dreamine.Communication.Sockets.Exceptions;

/// <summary>
/// \brief 소켓 통신 계층에서 발생하는 예외입니다.
/// </summary>
public sealed class SocketCommunicationException : CommunicationException
{
    /// <summary>
    /// \brief SocketCommunicationException 클래스의 새 인스턴스를 초기화합니다.
    /// </summary>
    public SocketCommunicationException()
    {
    }

    /// <summary>
    /// \brief 지정한 오류 메시지를 사용하여 SocketCommunicationException 클래스의 새 인스턴스를 초기화합니다.
    /// </summary>
    /// <param name="message">오류 메시지입니다.</param>
    public SocketCommunicationException(string message)
        : base(message)
    {
    }

    /// <summary>
    /// \brief 지정한 오류 메시지와 내부 예외를 사용하여 SocketCommunicationException 클래스의 새 인스턴스를 초기화합니다.
    /// </summary>
    /// <param name="message">오류 메시지입니다.</param>
    /// <param name="innerException">내부 예외입니다.</param>
    public SocketCommunicationException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}