# Java Projects

[![mini-redis](https://github.com/digvijay165/java-projects/actions/workflows/mini-redis.yml/badge.svg)](https://github.com/digvijay165/java-projects/actions/workflows/mini-redis.yml)
![Java 21](https://img.shields.io/badge/Java-21-orange?logo=openjdk&logoColor=white)

Systems built from scratch in plain Java, to learn how the tools I use every day work inside. No frameworks: just the JDK, sockets, files and threads, each project with tests and real benchmark numbers.

## Projects

| # | Project | What it is | Highlights |
|---|---|---|---|
| 1 | [**mini-redis**](mini-redis) | A Redis-compatible key-value server | One NIO event-loop thread, RESP protocol with pipelining, 39 commands, key expiry, crash-safe append-only file, 68 tests, 320k+ requests/sec pipelined |

## Coming next

- **HTTP server** on raw sockets, with a thread pool, keep-alive and routing
- **Stock exchange matching engine** with an order book and price-time priority
- **SQL database engine** with a parser, disk pages and a B+ tree index
- **Interpreter** for a small programming language
- **Git clone** with content-addressed storage and merges
- **Raft consensus** across several nodes, with a fault simulator

## Running a project

Each project is its own Maven build. You need Java 21 or newer and Maven.

```bash
cd mini-redis
mvn verify
```

Each project's README explains how it works, what it supports and how it was tested.
