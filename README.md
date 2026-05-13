# Dreamine.Communication.Sockets

`Dreamine.Communication.Sockets` is part of the Dreamine Communication package family.

This package provides TCP socket transport implementations while keeping socket-specific connection logic isolated from the application layer and from other transport packages.

[➡️ 한국어 문서 보기](./README_KO.md)

## Description

TCP socket transport package for Dreamine Communication. It provides `TcpClientTransport` and `TcpServerTransport` implementations that send and receive `MessageEnvelope` instances through a selected protocol adapter and frame codec.

## Package Role

```text
Dreamine.Communication.Abstractions
    ↑
Dreamine.Communication.Core
    ↑
Dreamine.Communication.Sockets
```

`Sockets` owns only the concrete TCP communication boundary:

- Opening and closing TCP client connections.
- Starting and stopping TCP server listeners.
- Accepting TCP clients.
- Running receive loops over `NetworkStream`.
- Broadcasting server messages to connected clients.
- Encoding outgoing `MessageEnvelope` instances through an `IMessageProtocolAdapter`.
- Splitting or writing stream data through an `IMessageFrameCodec`.

It must not own UI state, application command rules, business routing policies, serial port logic, RabbitMQ connection logic, or WPF-specific logic.

## Features

- TCP client transport
- TCP server transport
- `MessageEnvelope` based send and receive flow
- Configurable protocol adapter
- Configurable frame codec
- Shared JSON serialization and protocol adapters from `Core`
- Shared stream framing from `Core`
- Async receive loop with connection state management
- Server-side broadcast send to connected clients

## Main Components

| Type | Role |
|---|---|
| `TcpClientTransport` | `IMessageTransport` implementation for TCP client connections. |
| `TcpServerTransport` | `IMessageTransport` implementation for TCP server listener and connected clients. |
| `TcpClientTransportOptions` | TCP client connection configuration model. |
| `TcpServerTransportOptions` | TCP server listener configuration model. |
| `SocketCommunicationException` | Socket communication specific exception type. |

## TcpClientTransport

`TcpClientTransport` implements the shared `IMessageTransport` contract.

Default constructor behavior:

```text
Protocol adapter : DreamineEnvelopeProtocolAdapter
Frame codec      : LengthPrefixedMessageFrameCodec
```

This default is correct for Dreamine-to-Dreamine TCP communication. For external tools, TCP terminals, PLC gateway software, inspection tools, or raw string tests, pass the protocol adapter and frame codec explicitly.

Example raw text client:

```csharp
using System.Text;
using Dreamine.Communication.Core.Framing;
using Dreamine.Communication.Core.Protocols;
using Dreamine.Communication.Sockets.Clients;
using Dreamine.Communication.Sockets.Options;

var client = new TcpClientTransport(
    new TcpClientTransportOptions
    {
        Host = "127.0.0.1",
        Port = 15002
    },
    new PlainTextProtocolAdapter(
        Encoding.UTF8,
        "tcp.raw.available",
        "Tcp.RawAvailable"),
    new RawAvailableMessageFrameCodec());

client.MessageReceived += (_, message) =>
{
    var text = Encoding.UTF8.GetString(message.Payload);
    Console.WriteLine($"RX: {text}");
};

await client.ConnectAsync();
```

## TcpServerTransport

`TcpServerTransport` implements the shared `IMessageTransport` contract.

Default constructor behavior:

```text
Protocol adapter : DreamineEnvelopeProtocolAdapter
Frame codec      : LengthPrefixedMessageFrameCodec
```

The server starts a `TcpListener`, accepts multiple TCP clients, creates a receive loop per client, and sends outgoing messages to all currently connected clients.

Example raw text server:

```csharp
using System.Text;
using Dreamine.Communication.Core.Framing;
using Dreamine.Communication.Core.Protocols;
using Dreamine.Communication.Sockets.Options;
using Dreamine.Communication.Sockets.Servers;

var server = new TcpServerTransport(
    new TcpServerTransportOptions
    {
        Host = "0.0.0.0",
        Port = 15002
    },
    new PlainTextProtocolAdapter(
        Encoding.UTF8,
        "tcp.raw.available",
        "Tcp.RawAvailable"),
    new RawAvailableMessageFrameCodec());

server.MessageReceived += (_, message) =>
{
    var text = Encoding.UTF8.GetString(message.Payload);
    Console.WriteLine($"RX: {text}");
};

await server.ConnectAsync();
```

## TcpClientTransportOptions

| Option | Default | Meaning |
|---|---:|---|
| `Host` | `127.0.0.1` | TCP server host to connect to. |
| `Port` | `0` | TCP server port. Must be between 1 and 65535 before connection. |
| `ReceiveBufferSize` | `8192` | TCP receive buffer size. |
| `SendBufferSize` | `8192` | TCP send buffer size. |
| `ConnectTimeoutMs` | `5000` | TCP connection timeout in milliseconds. |

## TcpServerTransportOptions

| Option | Default | Meaning |
|---|---:|---|
| `Host` | `0.0.0.0` | IP address to bind. `0.0.0.0` means all network interfaces. |
| `Port` | `5000` | TCP listening port. Must be between 1 and 65535. |
| `Backlog` | `100` | TCP listener backlog. |
| `ReceiveBufferSize` | `8192` | Receive buffer size applied to accepted clients. |
| `SendBufferSize` | `8192` | Send buffer size applied to accepted clients. |

Server host parsing currently supports:

- `0.0.0.0`
- `127.0.0.1`
- `localhost`
- direct IP address strings

For a public service, bind the server to `0.0.0.0` or a specific local interface IP. For local testing, use `127.0.0.1`.

## Protocol Adapter and Frame Codec

TCP is stream-based. It does not preserve application message boundaries. Therefore message meaning and message boundary are intentionally separated:

| Responsibility | Type |
|---|---|
| Convert bytes to/from `MessageEnvelope` | `IMessageProtocolAdapter` |
| Detect message boundaries on a stream | `IMessageFrameCodec` |
| Open, close, send, receive through TCP socket | `TcpClientTransport` / `TcpServerTransport` |

## Recommended Combinations

| Scenario | Protocol adapter | Frame codec |
|---|---|---|
| Dreamine-to-Dreamine TCP communication | `DreamineEnvelopeProtocolAdapter` | `LengthPrefixedMessageFrameCodec` |
| Text protocol ending each message with `CRLF` or `LF` | `PlainTextProtocolAdapter` | `DelimiterMessageFrameCodec` |
| Simple TCP test tool sends raw text without delimiter | `PlainTextProtocolAdapter` | `RawAvailableMessageFrameCodec` |
| JSON text protocol with line ending | `RawJsonProtocolAdapter` | `DelimiterMessageFrameCodec` |
| Custom binary TCP protocol | Custom protocol adapter | Custom frame codec |

## RawAvailableMessageFrameCodec Support

`RawAvailableMessageFrameCodec` is defined in `Dreamine.Communication.Core`, not in this package. `TcpClientTransport` and `TcpServerTransport` can still use it because both transports accept any `IMessageFrameCodec`.

This mode is useful when a TCP tool or external device sends a plain value such as:

```text
test1
```

without:

- a Dreamine envelope
- a length prefix
- `CRLF`
- `LF`

Example setup:

```csharp
var transport = new TcpServerTransport(
    options,
    new PlainTextProtocolAdapter(
        Encoding.UTF8,
        "tcp.raw.available",
        "Tcp.RawAvailable"),
    new RawAvailableMessageFrameCodec());
```

This combination allows manual TCP tools such as Hercules-style clients to send a simple string and still trigger `MessageReceived`.

## Important TCP Stream Note

TCP is a byte stream. `RawAvailableMessageFrameCodec` reads the bytes returned by one stream read operation and treats them as one message.

That is convenient for manual testing, but it is not a strict message boundary rule. Depending on timing and buffer behavior, multiple sends can be merged or a single send can be split.

Example possible receive result:

```text
test1test2
```

or:

```text
te
st1
```

For production TCP protocols, prefer one of these designs:

- Use `LengthPrefixedMessageFrameCodec` for Dreamine internal communication.
- Use `DelimiterMessageFrameCodec` with `CRLF`, `LF`, or another explicit delimiter.
- Use a fixed-length binary frame.
- Use a protocol-specific custom `IMessageFrameCodec`.

Use `RawAvailableMessageFrameCodec` for compatibility, debugging, manual testing, or tools that have no explicit framing rule.

## Connection State

Both socket transports report state through the shared `ConnectionState` enum.

| State | TcpClientTransport | TcpServerTransport |
|---|---|---|
| `Disconnected` | Client is disconnected. | Listener is stopped. |
| `Connecting` | Client is connecting. | Server listener is starting. |
| `Connected` | Client is connected and receive loop is active. | Listener is running and accept loop is active. |
| `Disconnecting` | Client is closing. | Server is stopping and closing clients. |
| `Faulted` | Connection or receive loop failed. | Listener, accept loop, or socket operation failed. |

## Send Behavior

`TcpClientTransport.SendAsync` sends one encoded frame to the connected server.

`TcpServerTransport.SendAsync` broadcasts one encoded frame to all currently connected clients. If a client is disconnected or fails during send, that client is removed from the server connection list.

## Error Handling

The transports validate options before opening sockets. Invalid settings fail early.

Examples:

- Empty `Host`
- `Port <= 0`
- `Port > 65535`
- Non-positive buffer sizes
- Non-positive client connect timeout
- Non-positive server backlog

Runtime socket errors may move the transport to `Faulted` state. Application-level recovery should dispose or disconnect the transport, correct the socket condition, and reconnect or restart the server.

## Usage Example: Dreamine-to-Dreamine TCP

```csharp
using Dreamine.Communication.Abstractions.Models;
using Dreamine.Communication.Sockets.Clients;
using Dreamine.Communication.Sockets.Options;
using Dreamine.Communication.Sockets.Servers;

var server = new TcpServerTransport(
    new TcpServerTransportOptions
    {
        Host = "127.0.0.1",
        Port = 15001
    });

server.MessageReceived += (_, message) =>
{
    Console.WriteLine($"Server RX: {message.Name}");
};

await server.ConnectAsync();

var client = new TcpClientTransport(
    new TcpClientTransportOptions
    {
        Host = "127.0.0.1",
        Port = 15001
    });

await client.ConnectAsync();

await client.SendAsync(new MessageEnvelope
{
    Name = "Sample.Ping",
    Route = "sample.tcp.ping",
    Payload = []
});
```

## Usage Example: CRLF Text TCP

```csharp
using System.Text;
using Dreamine.Communication.Core.Framing;
using Dreamine.Communication.Core.Protocols;
using Dreamine.Communication.Sockets.Clients;
using Dreamine.Communication.Sockets.Options;

var client = new TcpClientTransport(
    new TcpClientTransportOptions
    {
        Host = "127.0.0.1",
        Port = 15002
    },
    new PlainTextProtocolAdapter(
        Encoding.UTF8,
        "tcp.plaintext",
        "Tcp.PlainText"),
    new DelimiterMessageFrameCodec(
        "\r\n",
        Encoding.UTF8,
        1024 * 1024));

await client.ConnectAsync();
```

## Design Principles

- Keep concrete socket implementation isolated from upper layers.
- Depend on `Dreamine.Communication.Abstractions` contracts.
- Reuse `Core` protocol adapters and frame codecs.
- Separate socket connection control from payload interpretation.
- Keep package responsibilities small and explicit.
- Preserve one-way dependency flow.
- Allow future TCP protocols and custom codecs to be added without changing application logic.

## Dependencies

- `Dreamine.Communication.Abstractions`
- `Dreamine.Communication.Core`

## Target Framework

```text
net8.0
```

## Related Packages

- `Dreamine.Communication.Abstractions`
- `Dreamine.Communication.Core`
- `Dreamine.Communication.Sockets`
- `Dreamine.Communication.Serial`
- `Dreamine.Communication.RabbitMQ`
- `Dreamine.Communication.FullKit`
- `Dreamine.Communication.Wpf`

## Documentation Check Notes

The previous README already covered the package role, dependency list, target framework, and broad feature summary. The missing or weakly described areas were:

- `TcpServerTransport` existence and server-side behavior
- `TcpClientTransport` and `TcpServerTransport` constructor defaults
- `TcpClientTransportOptions` and `TcpServerTransportOptions` field-level explanation
- protocol adapter and frame codec responsibility split
- recommended TCP protocol combinations
- `RawAvailableMessageFrameCodec` usage with TCP communication
- TCP stream boundary warning
- connection state, send behavior, and error handling behavior
- practical usage examples

These items are now included.

## License

This project is licensed under the MIT License.
