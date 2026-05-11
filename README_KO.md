# Dreamine.Communication.Sockets

`Dreamine.Communication.Sockets`는 Dreamine Communication 계열 패키지의 일부입니다.

이 패키지는 소켓 기반 전송 구현체를 제공합니다. TCP 관련 책임을 애플리케이션 계층 및 다른 전송 패키지와 분리합니다.

[➡️ English Version](./README.md)

## Description

TCP socket transport package for Dreamine Communication.

## 주요 기능

- TCP 클라이언트 Transport
- MessageEnvelope 기반 송수신 흐름
- Core의 공통 JSON 직렬화 사용
- Core의 길이 접두사 기반 프레임 처리 사용

## 설계 원칙

- 구체 통신 구현체를 상위 레이어와 분리합니다.
- `Dreamine.Communication.Abstractions`의 계약에 의존합니다.
- 패키지 책임을 작고 명확하게 유지합니다.
- 단방향 의존성 흐름을 유지합니다.
- 향후 어댑터를 추가해도 애플리케이션 로직을 변경하지 않도록 합니다.

## 패키지 역할

```text
Dreamine.Communication.Abstractions
    ↑
Dreamine.Communication.Core
    ↑
Dreamine.Communication.Sockets
```

## 의존성

- `Dreamine.Communication.Abstractions`
- `Dreamine.Communication.Core`

## 대상 프레임워크

```text
net8.0
```

## 관련 패키지

- `Dreamine.Communication.Abstractions`
- `Dreamine.Communication.Core`
- `Dreamine.Communication.Sockets`
- `Dreamine.Communication.Serial`
- `Dreamine.Communication.RabbitMQ`
- `Dreamine.Communication.FullKit`
- `Dreamine.Communication.Wpf`

## 라이선스

이 프로젝트는 MIT 라이선스를 따릅니다.
