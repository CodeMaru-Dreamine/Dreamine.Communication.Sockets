using System.Net;
using Dreamine.Communication.Sockets.Enums;
using Dreamine.Communication.Sockets.Exceptions;
using Dreamine.Communication.Sockets.Options;
using Xunit;

namespace Dreamine.Communication.Sockets.Tests;

public sealed class SocketOptionsTests
{
    [Fact]
    public void TcpClientDefaults_AreStable()
    {
        var options = new TcpClientTransportOptions();

        Assert.Equal("127.0.0.1", options.Host);
        Assert.Equal(0, options.Port);
        Assert.Equal(8192, options.ReceiveBufferSize);
        Assert.Equal(8192, options.SendBufferSize);
        Assert.Equal(5000, options.ConnectTimeoutMs);
    }

    [Fact]
    public void TcpServerDefaults_AreStable()
    {
        var options = new TcpServerTransportOptions();

        Assert.Equal("0.0.0.0", options.Host);
        Assert.Equal(5000, options.Port);
        Assert.Equal(100, options.Backlog);
        Assert.Equal(TcpServerSendTargetMode.Broadcast, options.SendTargetMode);
    }

    [Fact]
    public void UdpDefaults_CreateLoopbackEndpoints()
    {
        var options = new UdpTransportOptions();

        Assert.Equal(new IPEndPoint(IPAddress.Loopback, 16001), options.CreateLocalEndPoint());
        Assert.Equal(new IPEndPoint(IPAddress.Loopback, 16002), options.CreateRemoteEndPoint());
    }

    [Theory]
    [InlineData("0.0.0.0", "0.0.0.0")]
    [InlineData("localhost", "127.0.0.1")]
    [InlineData("192.0.2.10", "192.0.2.10")]
    public void LocalEndpoint_ParsesSupportedHosts(string host, string expected)
    {
        var options = new UdpTransportOptions { LocalHost = host, LocalPort = 12345 };

        var endpoint = options.CreateLocalEndPoint();

        Assert.Equal(IPAddress.Parse(expected), endpoint.Address);
        Assert.Equal(12345, endpoint.Port);
    }

    [Theory]
    [InlineData("")]
    [InlineData("not-an-ip-address")]
    public void RemoteEndpoint_RejectsInvalidHost(string host)
    {
        var options = new UdpTransportOptions { RemoteHost = host };

        Assert.Throws<ArgumentException>(() => options.CreateRemoteEndPoint());
    }

    [Fact]
    public void Options_RemainMutableForConfigurationBinding()
    {
        var client = new TcpClientTransportOptions
        {
            Host = "192.0.2.20",
            Port = 7000,
            ReceiveBufferSize = 4096,
            SendBufferSize = 2048,
            ConnectTimeoutMs = 2500
        };
        var udp = new UdpTransportOptions
        {
            EnableBroadcast = true,
            ReuseAddress = true
        };

        Assert.Equal("192.0.2.20", client.Host);
        Assert.Equal(7000, client.Port);
        Assert.Equal(4096, client.ReceiveBufferSize);
        Assert.Equal(2048, client.SendBufferSize);
        Assert.Equal(2500, client.ConnectTimeoutMs);
        Assert.True(udp.EnableBroadcast);
        Assert.True(udp.ReuseAddress);
    }

    [Fact]
    public void SocketExceptionSupportsAllConstructors()
    {
        var inner = new IOException("socket");

        Assert.NotEmpty(new SocketCommunicationException().Message);
        Assert.Equal("failed", new SocketCommunicationException("failed").Message);
        Assert.Same(inner, new SocketCommunicationException("failed", inner).InnerException);
    }
}
