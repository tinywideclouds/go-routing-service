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
    * **If user is "offline"**:
        1. The processor enqueues the message to the durable **Cold Queue** (Firestore).
        2. It publishes a **Notification Command** (containing just the User ID and Envelope) to the `push-notifications` Pub/Sub topic.
        3. The separate **Notification Service** consumes this command, resolves the user's device tokens, and handles the actual delivery via FCM/APNS.

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

```json
{
  "messages": [
    {
      "id": "a7b9c1-router-uuid-1",
      "envelope": { ... }
    },
    {
      "id": "f4d4e6-router-uuid-2",
      "envelope": { ... }
    }
  ]
}
```