# **go-routing-service**

The **go-routing-service** is a high-performance, secure, and scalable microservice responsible for real-time and asynchronous delivery of messages. It intelligently routes messages to online users via WebSockets and provides transient, Firestore-backed storage for offline users, following a "dumb router, smart client" (Sealed Sender) model.

### **Architecture**

The service is built on the go-microservice-base library and features a robust, scalable architecture composed of two primary services that can be scaled independently.

* **Stateless API & Processing Service**: A standard HTTP server for message ingestion (`POST /send`), retrieval of offline messages, and observability. This service also runs the core background pipeline that processes all incoming messages.
* **Stateful WebSocket Service**: A dedicated server that manages persistent WebSocket connections, tracks real-time user presence in a shared cache (e.g., Redis, Firestore), and handles direct message delivery.

These services are decoupled via Google Cloud Pub/Sub, enabling high availability and horizontal scaling.

#### **Message Ingestion & Routing**

1.  A client sends a "dumb" `SecureEnvelope` (with no sender metadata) to `POST /api/send`.
2.  The message is published to an ingress topic on Pub/Sub.
3.  The background processing pipeline consumes the message.
4.  The processor checks the `PresenceCache` for the recipient.
5.  If the user is **online**, the message is forwarded to the real-time **Delivery Bus** for immediate WebSocket delivery.
6.  Regardless of online status, the message is **always** stored in Firestore for offline/multi-device synchronization. (The `routing_processor.go` shows this logic).

#### **Offline Retrieval (Reliable Dumb Queue)**

The service implements a "Paginate-Save-Ack-Delete" model to ensure reliable, at-least-once delivery for offline clients.

1.  **Client (PULL):** A client comes online and calls `GET /api/messages?limit=100`.
2.  **Server (SENDS BATCH):** The server queries Firestore for the 100 *oldest* queued messages for that user and returns them as a `QueuedMessageList` (which includes the router-generated `id` and the `SecureEnvelope`).
3.  **Client (SAVES):** The client receives the batch and saves all 100 envelopes to its local database.
4.  **Client (ACK):** *After* saving, the client calls `POST /api/messages/ack` with the list of `id`s it has safely stored (e.g., `{"messageIds": ["uuid-a", "uuid-b", ...]}`).
5.  **Server (DELETES):** The server receives the ACK and performs a bulk delete in the background to remove those messages from the user's queue.
6.  The client repeats this process until the queue is empty.

---

### **API Endpoints**

#### `POST /api/send`

Ingests a single secure message for routing. The request body must be a JSON representation of the "dumb" `SecureEnvelope`.

* **Request Body:** `secure.SecureEnvelope`
* **Response:**
    * `202 Accepted`: The message has been accepted for processing.

#### `GET /api/messages`

Retrieves the next available batch of queued messages for the authenticated user.

* **Query Parameters:**
    * `limit` (optional): The maximum number of messages to return. (e.g., `?limit=50`). Defaults to `50`, max `500`.
* **Response:**
    * `200 OK`: Returns a `routing.QueuedMessageList` JSON object.

*Example Response Body:*

```json
{
  "messages": [
    {
      "id": "a7b9c1-router-uuid-1",
      "envelope": {
        "recipientId": "urn:sm:user:test-user-id",
        "encryptedData": "AE...D=",
        "encryptedSymmetricKey": "B...E=",
        "signature": "C...F="
      }
    },
    {
      "id": "f4d4e6-router-uuid-2",
      "envelope": {
        "recipientId": "urn:sm:user:test-user-id",
        "encryptedData": "ZE...A=",
        "encryptedSymmetricKey": "Y...B=",
        "signature": "X...C="
      }
    }
  ]
}
````