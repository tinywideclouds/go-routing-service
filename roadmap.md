
# Technical Roadmap: Real-Time Signaling Architecture

**Component:** Routing Service (Connection Manager)
**Context:** "Poke-then-Pull" Signal Bridge

## 1. Executive Summary

The Routing Service has transitioned to a "Smart Client" architecture where connected clients stop polling and rely on server-sent "Pokes" (wake-up signals) via WebSockets to initiate data fetches.

This requires a **Signal Bridge** to route "Poke" events from the backend (Routing Processor) to the specific Server Instance holding the user's WebSocket connection. Since WebSocket connections are stateful (pinned to a specific server) but Google Pub/Sub is stateless, we require a routing strategy to bridge this gap.

This document outlines the two-phase strategy for implementing this bridge, balancing immediate simplicity with long-term scalability.

---

## 2. Phase 1: Broadcast "Fan-Out" (Current Implementation)

### Architecture

In this phase, we treat the cluster as a homogeneous mesh. We do not attempt to track which server holds which connection globally. Instead, we broadcast every signal to every server and let the servers filter locally.

### Mechanism

1. **Publisher:** The Routing Processor publishes a poke `{"recipient": "urn:user:bob"}` to the shared topic `push-notifications`.
2. **Subscription:** Upon startup, every **Routing Service Instance** creates a unique, ephemeral subscription (e.g., `poke-sub-instance-uuid`) attached to that topic.
3. **Delivery:** Google Pub/Sub replicates the message and delivers a copy to *every* active instance.
4. **Local Filter:** Each instance receives the message, checks its local `SessionMap` for "Bob", and either routes the signal down the WebSocket or drops the message (No-Op).

### Why this choice?

* **Zero Coordination:** No service discovery, leader election, or external state store (Redis/Zookeeper) is required to track "which user is on which server."
* **Cloud Native:** Compatible with dynamic environments (Cloud Run/K8s) where IPs change and instances are ephemeral.
* **Robustness:** If an instance crashes, its subscription expires. If it restarts, it gets a fresh ID. No "stale routing tables" to debug.

### Performance Characteristics

* **Cost Model:** Linear. Total Operations = `(Messages/Sec) * (N Instances)`.
* **Latency:** Minimal (standard Pub/Sub latency).

---

## 3. The Scaling Trigger (When to Switch)

Phase 1 is efficient at low-to-medium scale but wasteful at high scale due to message duplication.

### The Formula

The system bottleneck is determined by the **Noise Ratio**—the percentage of CPU time a server spends deserializing messages destined for *other* servers.

### The Red Line

We recommend initiating the transition to Phase 2 when **Global Ops/Sec exceeds 5,000**.

* *Example Safe:* 100 msgs/sec \times 10 Servers = 1,000 Ops/Sec (Phase 1 is fine).
* *Example Critical:* 1,000 msgs/sec \times 50 Servers = 50,000 Ops/Sec.
* At this point, every server is processing 50,000 JSON payloads per second, 98% of which are discarded. This wastes CPU and bandwidth.



---

## 4. Phase 2: Partitioned "Sticky" Routing (Long Term)

When the Red Line is crossed, we transition to a **Partitioned Architecture**. We do not abandon Pub/Sub; instead, we leverage **Pub/Sub Filtering** to offload the routing logic to Google's infrastructure.

### Mechanism

1. **Sharding:** The user base is logically divided into **Shards** (e.g., 0–100) based on the hash of their URN.
2. **Smart Publisher:** The Routing Processor calculates the shard ID (`crc32("bob") % 100`) and attaches it as a message attribute: `attributes: {"shard": "42"}`.
3. **Filtered Subscriptions:**
* Server instances no longer subscribe to "everything."
* A **Coordinator** assigns specific shards to specific servers (e.g., "Instance A owns Shard 42").
* Instance A creates its subscription with a **Filter**: `attributes.shard = "42"`.


4. **Delivery:** Google Pub/Sub *only* delivers the message to the subscriber handling Shard 42.

### The Cost of Phase 2

This phase introduces significant operational complexity:

* **Coordination Layer:** Requires a mechanism (e.g., Kubernetes StatefulSet, Consistent Hashing ring, or a separate "Coordinator Service") to ensure every shard is owned by exactly one active server.
* **Rebalancing:** If Server A dies, Shard 42 must be instantly reassigned to Server B, or users on that shard go silent.

---

## 5. Migration Strategy & Technical Debt

### Reusability Analysis

The code implemented for Phase 1 is **90% reusable** for Phase 2.

| Component | Phase 1 (Fan-Out) | Phase 2 (Partitioned) | Rework Required |
| --- | --- | --- | --- |
| **Publisher** | Basic Publish | Add `shard` attribute | Minimal |
| **Consumer Logic** | `handlePoke()` | `handlePoke()` | **None (Reused)** |
| **WebSocket Mgmt** | `SessionMap` lookup | `SessionMap` lookup | **None (Reused)** |
| **Infrastructure** | Ephemeral Sub (No Filter) | Ephemeral Sub (With Filter) | Config Change |
| **Orchestration** | None | **Coordinator Service** | **New Component** |
