using System;
using Dreamine.Communication.Abstractions.Exceptions;

namespace Dreamine.Communication.Sockets.Exceptions;

/// <summary>
/// \if KO
/// <para>TCP 또는 UDP 소켓 연결과 송수신 과정에서 발생한 통신 오류를 나타냅니다.</para>
/// \endif
/// \if EN
/// <para>Represents a communication error raised during TCP or UDP socket connection and transfer.</para>
/// \endif
/// </summary>
public sealed class SocketCommunicationException : CommunicationException
{
    /// <summary>
    /// \if KO
    /// <para>기본 메시지로 새 소켓 통신 예외를 초기화합니다.</para>
    /// \endif
    /// \if EN
    /// <para>Initializes a new socket communication exception with the default message.</para>
    /// \endif
    /// </summary>
    public SocketCommunicationException()
    {
    }

    /// <summary>
    /// \if KO
    /// <para>지정한 오류 메시지로 새 소켓 통신 예외를 초기화합니다.</para>
    /// \endif
    /// \if EN
    /// <para>Initializes a new socket communication exception with the specified message.</para>
    /// \endif
    /// </summary>
    /// <param name="message">
    /// \if KO
    /// <para>오류 원인을 설명하는 메시지입니다.</para>
    /// \endif
    /// \if EN
    /// <para>The message describing the error.</para>
    /// \endif
    /// </param>
    public SocketCommunicationException(string message)
        : base(message)
    {
    }

    /// <summary>
    /// \if KO
    /// <para>지정한 오류 메시지와 내부 예외로 새 소켓 통신 예외를 초기화합니다.</para>
    /// \endif
    /// \if EN
    /// <para>Initializes a new socket communication exception with a message and inner exception.</para>
    /// \endif
    /// </summary>
    /// <param name="message">
    /// \if KO
    /// <para>오류 원인을 설명하는 메시지입니다.</para>
    /// \endif
    /// \if EN
    /// <para>The message describing the error.</para>
    /// \endif
    /// </param>
    /// <param name="innerException">
    /// \if KO
    /// <para>현재 오류의 원인이 된 예외입니다.</para>
    /// \endif
    /// \if EN
    /// <para>The exception that caused the current error.</para>
    /// \endif
    /// </param>
    public SocketCommunicationException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
