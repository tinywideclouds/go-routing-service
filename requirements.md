## **🏛️ L1: Business & Mission Requirements**

These are the high-level strategic goals of the service.

* **L1.1:** The service **shall** accept and securely ingest messages for routing.
* **L1.2:** The service **shall** provide a unified, client-driven API for at-least-once message retrieval.
* **L1.3:** The service **shall** intelligently route messages to transient (hot) or durable (cold) storage based on real-time user presence.
* **L1.4:** The service **shall** ensure data persistence by migrating transient messages to durable storage upon user disconnect to prevent data loss.

---

## **Functional & Feature Requirements**

These features break down the L1 goals into specific, testable capabilities.

### **L2.1: Message Ingestion Flow (from L1.1)**

* **L2.1.1:** The API **shall** provide an authenticated POST /api/send endpoint to accept a secure.SecureEnvelope.
* **L2.1.2:** The API **shall** publish the SecureEnvelope to a Pub/Sub Ingress Topic for asynchronous processing.
* **L2.1.3:** A dataflow pipeline **shall** consume from the Ingress Topic.
* **L2.1.4:** The pipeline **shall** transform and validate the raw message, ensuring it is a valid SecureEnvelope with a parseable RecipientID.

### **L2.2: Unified Retrieval Flow (from L1.2)**

* **L2.2.1:** The API **shall** provide an authenticated GET /api/messages endpoint for clients to pull their message batch.
* **L2.2.2:** The API **shall** provide an authenticated POST /api/messages/ack endpoint for clients to confirm receipt and trigger message deletion.
* **L2.2.3:** The GET /api/messages request **shall** query the Hot Queue first. If the Hot Queue is empty, it **shall** then query the Cold Queue.
* **L2.2.4:** The POST /api/messages/ack endpoint **shall** return an immediate 204 No Content and delete the acknowledged messages from the correct queue(s) in the background.

### **L2.3: Intelligent Routing Logic (from L1.3)**

* **L2.3.1:** The routing processor **shall** query the PresenceCache to determine if the recipient is "online" or "offline".
* **L2.3.2:** For an "online" user, the processor **shall** enqueue the message to the **Hot Queue**.
* **L2.3.3:** For an "online" user, the processor **shall** trigger a "poke" notification (a generic signal, *not* the message data) to the client's active connection.
* **L2.3.4:** For an "offline" user, the processor **shall** enqueue the message to the **Cold Queue**.
* **L2.3.5:** For an "offline" user, the processor **shall** fetch device tokens and trigger a mobile push notification.

### **L2.4: Presence & Data Persistence (from L1.4)**

* **L2.4.1:** The ConnectionManager **shall** provide an authenticated GET /connect endpoint to upgrade HTTP requests to a WebSocket.
* **L2.4.2:** Upon successful connection, the ConnectionManager **shall** register the user's presence in the PresenceCache.
* **L2.4.3:** Upon client disconnect, the ConnectionManager **shall** immediately delete the user's record from the PresenceCache.
* **L2.4.4:** Upon client disconnect, the ConnectionManager **shall** immediately trigger the MigrateHotToCold function to move all transient messages for that user to the durable Cold Queue.
* **L2.4.5:** If the Hot Queue fails during an "online" enqueue (e.g., Redis is down), the system **shall** automatically fall back to the Cold Queue to prevent data loss.

---

## **⚙️ L3: Technical & Non-Functional Requirements**

These are the specific "how-to" details that are directly implemented in the code.

### **L3.1: API & Handlers**

* **L3.1.1:** The POST /api/send endpoint **must** return 202 Accepted on success.
* **L3.1.2:** The GET /api/messages endpoint **must** support a limit query parameter, defaulting to 50 and capped at 500\.
* **L3.1.3:** The POST /api/messages/ack endpoint **must** accept a JSON body: {"messageIds": \["id1", "id2", ...\]}.
* **L3.1.4:** The POST /api/messages/ack database deletion **must** run in a background goroutine.

### **L3.2: Queue Implementation**

* **L3.2.1:** The system **must** provide HotQueue and ColdQueue interfaces, which are governed by a unified MessageQueue interface.
* **L3.2.2:** The Hot Queue implementation (both Redis and Firestore) **must** use a "pending" list/collection. RetrieveBatch atomically moves items from "main" to "pending".
* **L3.2.3:** The Acknowledge function **must** delete messages from the "pending" list/collection.
* **L3.2.4:** The MigrateHotToCold function **must** move messages from *both* the "main" and "pending" hot queues to the cold queue.
* **L3.2.5:** The Hot Queue implementation **must** be pluggable (supporting "redis" and "firestore") via config.yaml.

### **L3.3: Pipeline & Routing**

* **L3.3.1:** The "poke" notification (L2.3.3) **must** be implemented by calling PushNotifier.Notify with a nil envelope.
* **L3.3.2:** The mobile push notification payload (L2.3.5) **must** be "dumb," containing only generic text (e.g., "You have received a new secure message") and **must not** contain any data from the SecureEnvelope.

### **L3.4: General NFRs (Non-Functional Requirements)**

* **L3.4.1:** The service **must** support graceful shutdown, listening for SIGINT and SIGTERM signals.
* **L3.4.2:** All services (API, WebSocket) **must** be able to be dynamically assigned a port (:0) and report the bound port for testing.
* **L3.4.3:** The entire "Hot/Cold/Migration" flow **must** be validated by an end-to-end test that simulates a real user flow against emulators.