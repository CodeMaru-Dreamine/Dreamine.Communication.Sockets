# Dreamine.Communication.Sockets

`Dreamine.Communication.Sockets` is part of the Dreamine Communication package family.

This package provides socket-based transport implementations. It keeps TCP concerns isolated from the application layer and from other transport packages.

[➡️ 한국어 문서 보기](README_ko.md)

## Description

TCP socket transport package for Dreamine Communication.

## Features

- TCP client transport
- MessageEnvelope based send and receive flow
- Shared JSON serialization from Core
- Shared length-prefixed framing from Core

## Design Principles

- Keep concrete transport implementations isolated from upper layers.
- Depend on `Dreamine.Communication.Abstractions` contracts.
- Keep package responsibilities small and explicit.
- Preserve one-way dependency flow.
- Allow future adapters to be added without changing application logic.

## Package Role

```text
Dreamine.Communication.Abstractions
    ↑
Dreamine.Communication.Core
    ↑
Dreamine.Communication.Sockets
```

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

## License

This project is licensed under the MIT License.
