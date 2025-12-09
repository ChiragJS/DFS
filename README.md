# Distributed File System

This project is a simplified Distributed File System inspired by the architecture of systems like GFS and HDFS. It is designed to demonstrate how a distributed storage layer can be structured, including a master node that manages metadata and multiple chunk servers that store data.

The goal is to build a fully functional DFS that supports file uploads, downloads, metadata management, chunk replication, heartbeats, and write pipelines.

---

## Table of Contents

* [Overview](#overview)
* [Features](#features)
* [Architecture](#architecture)
* [Component Diagram](#component-diagram)
* [Write Sequence](#write-sequence)
* [Read Sequence](#read-sequence)
* [ChunkServer Heartbeats](#chunkserver-heartbeats)
* [Master Node Internal Architecture](#master-node-internal-architecture)

---

## Overview

The system consists of:

* A Client
* A Master Node
* Multiple ChunkServers

Data flow:

1. Client asks the master for metadata or chunk placement.
2. Master returns assigned chunkservers.
3. Client uploads/downloads data directly from chunkservers.

---

## Features

### Implemented

* File upload
* File download
* Basic master–client communication
* Metadata management
* Heartbeats
* Replica placement

### To Be Implemented

* Chunking
* Write pipeline (primary + replicas)
* Lease management
* Master internal subsystems ( code isn't clean rn , will focus on it after getting the basic proto up )

---

## Architecture

The system follows a master–worker model. The master stores metadata, while chunkservers store data. The client interacts with both.

---

## Component Diagram

```mermaid
flowchart TB
    classDef box fill:#2b2b2b,stroke:#555,color:#fff,stroke-width:2px,rx:8,ry:8,font-size:16px
    classDef arrow stroke:#999,color:#ccc,stroke-width:2px

    Client["Client<br/><br/>CLI / API<br/><br/>Issues file operations"]:::box -->|gRPC| MasterNode

    subgraph MasterNode["Master Node"]
        MM["Metadata Manager<br/><br/>File tree<br/>File → Chunk mapping"]:::box
        HM["Heartbeat Manager<br/><br/>Monitor ChunkServers"]:::box
        CM["Chunk Placement Manager<br/><br/>Replica selection"]:::box
        LM["Lease Manager<br/><br/>Primary write control"]:::box
    end

    subgraph CSGroup["ChunkServers"]
        CS1["ChunkServer 1<br/><br/>Stores chunks<br/>Heartbeats<br/>Replication"]:::box
        CS2["ChunkServer 2<br/><br/>Stores chunks<br/>Heartbeats<br/>Replication"]:::box
        CS3["ChunkServer 3<br/><br/>Stores chunks<br/>Heartbeats<br/>Replication"]:::box
        CSN["ChunkServer N<br/><br/>Stores chunks<br/>Heartbeats<br/>Replication"]:::box
    end

    MasterNode -->|gRPC| CS1
    MasterNode -->|gRPC| CS2
    MasterNode -->|gRPC| CS3
    MasterNode -->|gRPC| CSN

    Client -->|Read/Write Data| CS1
    Client -->|Read/Write Data| CS2
    Client -->|Read/Write Data| CS3
    Client -->|Read/Write Data| CSN
```

---

## Write Sequence

```mermaid
sequenceDiagram
    autonumber
    participant C as Client
    participant M as Master
    participant P as PrimaryChunkServer
    participant R1 as Replica1
    participant R2 as Replica2

    C->>M: Request file write
    M-->>C: Return chunk assignment

    C->>P: Send write data
    P->>R1: Forward write
    P->>R2: Forward write

    R1-->>P: Ack
    R2-->>P: Ack

    P-->>C: Write success
```

---

## Read Sequence

```mermaid
sequenceDiagram
    autonumber
    participant C as Client
    participant M as Master
    participant CS as ChunkServer

    C->>M: Request metadata
    M-->>C: Return chunk locations

    C->>CS: Fetch chunk
    CS-->>C: Return data
```

---

## ChunkServer Heartbeats

```mermaid
sequenceDiagram
    autonumber
    participant CS as ChunkServer
    participant M as Master

    loop Every 5 seconds
        CS->>M: Heartbeat
        M-->>CS: Commands
    end
```

---

## Master Node Internal Architecture

```mermaid
flowchart LR
    classDef box fill:#2b2b2b,stroke:#555,color:#fff,stroke-width:1,rx:6,ry:6

    subgraph Master["Master Node"]
        RPC["gRPC Server<br/><br/>Handles Client & CS RPCs"]:::box
        FS["File Namespace Manager<br/><br/>Directory tree"]:::box
        MM["Chunk Metadata Store<br/><br/>File → Chunk → Replicas"]:::box
        HB["Heartbeat Manager<br/><br/>Node monitoring"]:::box
        CP["Chunk Placement<br/><br/>Replica selection<br/>Load balancing"]:::box
        LM["Lease Manager<br/><br/>Primary write control"]:::box
    end

    RPC --> FS
    RPC --> MM
    RPC --> HB
    RPC --> CP
    RPC --> LM
```

---

This document will evolve as implementation progresses.
