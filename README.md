# **go-routing-service**

The **go-routing-service** is a high-performance, pure router service for sealed sender messages. It is **not a message store**.

The service guarantees **at-least-once, in-order** delivery by intelligently routing messages to a high-speed **"hot" queue** for online users and a durable **"cold" queue** for offline users. It follows a "dumb router, smart client" model, where the client is responsible for all message retrieval and acknowledgment.

## **🏛️ Architecture**

The service is composed of two primary components that can be scaled independently:

* **Stateless API & Processing Service**: A standard HTTP server for message ingestion (POST /api/send), retrieval of queued messages (GET /api/messages), and acknowledgment (POST /api/messages/ack). This service also runs the background processing pipeline that consumes from Pub/Sub and routes messages.
* **Stateful WebSocket Service**: A dedicated server that manages persistent WebSocket connections and tracks real-time user presence in a shared cache (e.g., Firestore, Redis).

These components work together to provide a single, unified delivery flow.

---

### **📥 Message Ingestion & Routing**

1. A client sends a SecureEnvelope to POST /api/send.
2. The API service publishes the raw envelope to an **Ingress Topic** on Google Cloud Pub/Sub.
3. The background **Processing Pipeline** consumes the message.
4. The processor checks the PresenceCache for the recipient.
5. **Routing Logic:**
    * **If user is "online"**: The processor enqueues the message to the high-speed **Hot Queue** (e.g., Redis). A lightweight "poke" notification is then sent to the client over its active WebSocket.
    * **If user is "offline"**: The processor enqueues the message to the durable **Cold Queue** (Firestore). A standard mobile push notification (FCM/APNS) is then triggered.

### **📤 Client Retrieval (The Unified Flow)**

This "Paginate-Save-Ack-Delete" model is the **only** way clients receive messages, unifying the online and offline paths.

1. **Client (PULL):**
    * An **offline** client comes online and *immediately* calls GET /api/messages.
    * An **online** client receives a "poke" and calls GET /api/messages.
2. **Server (SENDS BATCH):** The CompositeMessageQueue handles the request. It *first* checks the **Hot Queue** for messages. If the hot queue is empty, it *then* checks the **Cold Queue**. It returns the oldest available batch.
3. **Client (SAVES):** The client receives the batch and saves all envelopes to its local database.
4. **Client (ACK):** *After* saving, the client calls POST /api/messages/ack with the list of messageIds it has safely stored.
5. **Server (DELETES):** The CompositeMessageQueue receives the IDs and deletes them from the correct queue (hot or cold).
6. The client repeats this process until the queue is empty.

### **🔌 Presence & "Zombie" Connection Handling**

To prevent messages from being "stuck" in the hot queue if a client disconnects ungracefully (a "zombie" connection), the service uses the following flow:

1. **Connect:** A client opens a WebSocket to GET /connect. The ConnectionManager registers the connection and sets the user's presence to "online" in the PresenceCache.
2. **Disconnect:** The client disconnects. The ConnectionManager's Remove() hook is triggered.
3. **Migration:** The Remove() hook **immediately** does two things:
    * It deletes the user's "online" record from the PresenceCache.
    * It calls MigrateHotToCold, which atomically moves any and all messages for that user from the **Hot Queue** to the **Cold Queue**.

This guarantees that all in-flight messages are safely persisted for the user's next session.

---

## **📡 API Endpoints**

### **POST /api/send**

Ingests a single secure message for routing.

* **Request Body:** secure.SecureEnvelope
* **Response:**
    * 202 Accepted: The message has been accepted for processing.

### **GET /api/messages**

Retrieves the next available batch of queued messages for the authenticated user.

* **Query Parameters:**
    * limit (optional): The maximum number of messages to return. (Defaults to 50, max 500).
* **Response:**
    * 200 OK: Returns a routing.QueuedMessageList JSON object.

*Example Response Body:*

JSON
````
{  
"messages": \[  
{  
"id": "a7b9c1-router-uuid-1",  
"envelope": { ... }  
},  
{  
"id": "f4d4e6-router-uuid-2",  
"envelope": { ... }  
}  
\]  
}
````
### **POST /api/messages/ack**

Acknowledges that a batch of messages has been successfully received and persisted by the client. The server will delete the acknowledged messages from the queue.

* **Request Body:**  
  JSON  
  {  
  "messageIds": \["a7b9c1-router-uuid-1", "f4d4e6-router-uuid-2"\]  
  }

* **Response:**
    * 204 No Content: The acknowledgment was accepted.

### **GET /connect**

Upgrades the HTTP connection to a WebSocket for real-time presence tracking and "poke" notifications.

---

## **⚙️ Configuration**

The service is configured via a config.yaml file. Key properties include:

* ProjectID: The GCP project ID.
* ColdQueueCollection: The name of the Firestore collection for the durable "cold" queue.
* PresenceCache: Configuration for the pluggable presence cache (type: "firestore" or "redis").
* HotQueue: Configuration for the pluggable "hot" queue (type: "firestore" or "redis"). This allows using Redis in production while using a simpler Firestore-backed prototype for local development.


## **Architecture: Why "Poke-then-Pull"?**

### **A Pragmatic Analysis of the Unified Flow Model**

### **1\. The Core Principle: We Are a Router, Not a Store**

The single most important constraint driving the architecture of the go-routing-service is its definition: **It is a pure router, not a message store.**

This service guarantees **"at-least-once"** delivery. However, once a client has acknowledged a message (POST /api/messages/ack), that message is deleted and gone forever.

This constraint immediately invalidates the most common real-world architecture used for large-scale messaging.

### **2\. The Architectural Fork: The "Store-Backed" Model**

Most high-performance messaging apps (like Slack or Discord) use a "store-backed" hybrid model. This common architecture relies on two distinct flows:

* **Flow 1 (Fast):** A WebSocket pushes "best-effort" data. It's a "fire-and-forget" push for live UI updates. It doesn't handle acks or guarantees.
* **Flow 2 (Guaranteed):** A robust HTTP API (GET /messages) pulls data from a **permanent message store**.

This model is popular because it separates concerns: the WebSocket is for *speed*, and the API is for *truth*. If the client misses a "fast" message (e.g., packet loss), it doesn't matter. It will simply "catch up" by fetching the truth from the API.

The go-routing-service **cannot use this model**. Because we are not a message store, we have no permanent "truth" for the API to pull from. Our guarantee must happen in real-time.

This leaves two options.

### **3\. The "Unified Pull" vs. The "Dual Guaranteed"**

We must provide an "at-least-once" guarantee. The architectural choice comes down to *how* we implement this guarantee.

#### **Option A: The "Unified Pull" Model (The Adopted Solution)**

This is the **"poke-then-pull"** architecture currently in use. It is designed to provide the guarantee using a *single, unified, elegant* flow.

* **How it Works:**
    1. The service has **one** stateful, guaranteed delivery flow: the GET /api/messages and POST /api/messages/ack endpoints.
    2. The WebSocket connection is kept deliberately **"dumb."** Its *only* job is to track presence and send a stateless "poke" (a trigger).
    3. The client has **one** function for data (syncMessages()). It calls this function on initial connection (for "cold" messages) and *also* when it receives a "poke" (for "hot" messages).

The guarantee is handled entirely by the robust HTTP pull-and-ack mechanism. The real-time component is just a simple trigger to start that mechanism.

#### **Option B: The "Dual Guaranteed" Model (The Rejected Path)**

This model, which involves adding a deliver/ack cycle directly to the WebSocket, is rejected for one critical reason: **it creates two parallel, stateful, guaranteed delivery systems.**

This is the "duplicated deliver/ack" problem. The pragmatic reasons for rejecting it are severe:

* **Explosive Complexity:** The WebSocket ConnectionManager (currently "dumb") must be rebuilt into a "smart," stateful agent. It must manage timers, ack states (pending, acked, timed\_out), and retry logic for every message for every connected client.
* **The "Lost Ack" Race Condition:** This is the most dangerous failure mode.
    1. The server push(data) over the WebSocket.
    2. The client receives it, saves it, and sends ack(id).
    3. A brief network flap occurs, and the server **never receives the ack**.
    4. The server's timer expires. It *thinks* the delivery failed and moves the message to the "slow" API queue for later.
    5. The client later reconnects and calls GET /api/messages, receiving the **exact same message a second time.**
* **The Client Burden:** This race condition *forces the client* to build a complex de-duplication layer to protect itself from the server's complex failure modes.

### **4\. Analysis: The "Elegance vs. Optimization" Trade-Off**

The team deliberately chose **Option A** ("Poke-then-Pull"). This was a conscious trade-off, accepting a minor optimization gap to gain a major architectural advantage.

#### **The Optimization Gap (What We Lose)**

We lose a small amount of latency for online users.

* **Dual Guaranteed Model (Option B):** Direct Send (e.g., \~150ms)
* **Unified Pull Model (Option A):** Poke (\~50ms) \+ GET /api/messages (\~150ms) \= **\~200ms**

The cost of our architecture is **one extra HTTP round trip** (\~50ms) for every real-time message. In the context of this service, this latency is considered imperceptible and a negligible price to pay.

#### **The Simplicity Gain (What We Win)**

* **Server Elegance:** We have **one** place to manage guarantees, state, and delivery logic. The WebSocket component is simple, stateless, and easy to scale. The API component is robust and testable.
* **Client Simplicity:** The client has **one** function for receiving data. It does not need to build a complex de-duplication system to protect against server-side race conditions.
* **Reliability:** We have **eliminated an entire class of distributed system bugs** (like the "lost ack" race condition) by design.

### **Conclusion**

The "poke-then-pull" model is not a legacy pattern; it is a deliberate and elegant design that perfectly fits our core constraint of being a **router and not a store.**

It provides the required "at-least-once" guarantee in a single, robust, and testable flow, correctly trading a few milliseconds of latency for a massive gain in simplicity and reliability.