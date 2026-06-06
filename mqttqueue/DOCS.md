# MQTT Queue Forwarder

Reads messages from a local SQLite `mqtt_queue` table and publishes them to
an MQTT broker using QOS-2 (exactly-once delivery).  Up to 20 messages are
kept in-flight simultaneously so the broker's round-trip time does not limit
throughput.

This message forwarder is limited to about 14 msg/s in a realistic connection to a TLS AWS MQTT server using QOS 2 from a homelab server. I'm planning not to load it with more than 5 mqtt msg/s throughput, so that the service has throughput to spare to catch up after a connection error.

The scale we are using this queue forwarder at is of a local Home Assistant install forwarding over an unreliable connection to a central AWS MQTT server. So we don't expect more than 1 - 5 messages per second.

If you have bigger throughput queue needs you could check out 
* https://www.dbos.dev/
* https://github.com/NikolayS/pgque
* https://github.com/microsoft/pg_durable

---

## How it works

Another process (your application) writes rows into a SQLite database:

```sql
CREATE TABLE IF NOT EXISTS mqtt_queue (
    id      INTEGER PRIMARY KEY AUTOINCREMENT,
    ts      INTEGER NOT NULL,
    topic   TEXT    NOT NULL,
    message TEXT    NOT NULL,
    retain  BOOLEAN NOT NULL,
    sent    INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_queue_pending ON mqtt_queue (sent, ts);
```

This add-on reads unsent rows (oldest timestamp first) from a sqlite file, publishes each one to your MQTT broker at QOS-2, and marks the row `sent = 1` once the broker confirms delivery.  Rows that are not confirmed before a restart are
re-published automatically on the next run.

Here is a node-red flow to create and populate this sqlite file and table

```
[{"id":"ebf102d1e9aa17bf","type":"mqtt in","z":"a21d7bcd93ad4405","name":"local ha shellyemg3-e4b0","topic":"shellyemg3-e4b0/#","qos":"2","datatype":"auto","broker":"2ae2ce1a1d86db84","nl":false,"rap":false,"rh":0,"inputs":0,"x":200,"y":240,"wires":[["3c241445cc244428"]]},{"id":"3c241445cc244428","type":"function","z":"a21d7bcd93ad4405","name":"Format for sqlite","func":"const topic   = msg.topic || '_no_topic';\nconst message = (typeof msg.payload === 'object')\n    ? JSON.stringify(msg.payload)\n    : String(msg.payload);\nconst retain = msg.retain\n\n// Unix milliseconds – sortable integer, no timezone ambiguity\nconst timestamp = Date.now();\n\nmsg.params = {\n    $ts:timestamp,\n    $topic:topic,\n    $message:message,\n    $retain:retain\n}\n\nreturn msg;","outputs":1,"timeout":"","noerr":0,"initialize":"","finalize":"","libs":[],"x":490,"y":240,"wires":[["57255c551384a8a7"]]},{"id":"67f8b1331e0bee1b","type":"function","z":"a21d7bcd93ad4405","name":"Build Init SQL Sequence","func":"// Sends each statement individually through the sqlite node.\n// Node-RED queues them so they execute in strict order.\n// Safe to run multiple times – all statements are idempotent.\n\nconst statements = [\n\n    // 1. WAL mode – persists in the db file, only needs setting once,\n    //    but harmless to repeat. Allows concurrent readers + one writer.\n    'PRAGMA journal_mode=WAL',\n\n    // 2. Retry window – wait up to 30 s on a busy lock before erroring.\n    //    Must be set per-connection (does NOT persist in the file).\n    'PRAGMA busy_timeout=14000',\n\n    // 3. Recommended companion to WAL – safe and faster than FULL.\n    'PRAGMA synchronous=NORMAL',\n\n    // 4. Queue table\n    `CREATE TABLE IF NOT EXISTS mqtt_queue (\n        id        INTEGER PRIMARY KEY AUTOINCREMENT,\n        ts        INTEGER NOT NULL,\n        topic     TEXT    NOT NULL,\n        message   TEXT    NOT NULL,\n        retain    BOOLEAN NOT NULL,\n        sent      INTEGER NOT NULL DEFAULT 0\n    )`,\n\n    // 5. Composite index on (sent, timestamp) – makes the drain query\n    //    SELECT ... WHERE sent=0 ORDER BY timestamp fast even with\n    //    millions of rows, without scanning the whole table.\n    'CREATE INDEX IF NOT EXISTS idx_queue_pending ON mqtt_queue (sent, ts)'\n\n\n];\n\nconst msgs = [];\nfor (const sql of statements) {\n    msgs.push({ topic: sql, payload: [] });\n}\nreturn [ msgs ];","outputs":1,"timeout":"","noerr":0,"initialize":"","finalize":"","libs":[],"x":490,"y":360,"wires":[["699eef8b82d234a6"]]},{"id":"57255c551384a8a7","type":"sqlite","z":"a21d7bcd93ad4405","mydb":"1579549ab2551290","sqlquery":"prepared","sql":"INSERT INTO mqtt_queue (ts, topic, message, retain) VALUES ($ts,$topic,$message,$retain)","name":"INSERT INTO mqtt_queue","x":740,"y":240,"wires":[["d83faeba734fdce5"]]},{"id":"d83faeba734fdce5","type":"debug","z":"a21d7bcd93ad4405","name":"SQLite Result 2","active":false,"tosidebar":true,"console":false,"tostatus":true,"complete":"payload","targetType":"msg","statusVal":"payload","statusType":"auto","x":920,"y":300,"wires":[]},{"id":"ad199b4f419e2dd0","type":"catch","z":"a21d7bcd93ad4405","name":"Catch Errors","scope":null,"uncaught":false,"x":150,"y":480,"wires":[["76a0ce64baab9571"]]},{"id":"76a0ce64baab9571","type":"debug","z":"a21d7bcd93ad4405","name":"Write Error","active":true,"tosidebar":true,"console":true,"tostatus":false,"complete":"true","targetType":"full","statusVal":"","statusType":"auto","x":410,"y":480,"wires":[]},{"id":"699eef8b82d234a6","type":"sqlite","z":"a21d7bcd93ad4405","mydb":"1579549ab2551290","sqlquery":"msg.topic","sql":"INSERT INTO mqtt_queue (timestamp, topic, message) VALUES (?,?,?)","name":"via msg.topic","x":720,"y":360,"wires":[["c0a502b8bd4fab35"]]},{"id":"c0a502b8bd4fab35","type":"debug","z":"a21d7bcd93ad4405","name":"SQlite init response","active":false,"tosidebar":true,"console":false,"tostatus":true,"complete":"payload","targetType":"msg","statusVal":"payload","statusType":"auto","x":930,"y":360,"wires":[]},{"id":"a543abe95d6b2258","type":"inject","z":"a21d7bcd93ad4405","name":"Startup init db connection","props":[{"p":"payload"}],"repeat":"","crontab":"","once":true,"onceDelay":"0","topic":"","payload":"startup","payloadType":"str","x":200,"y":360,"wires":[["67f8b1331e0bee1b"]]},{"id":"60a09d6b9d90f95f","type":"mqtt out","z":"a21d7bcd93ad4405","name":"HA Discovery (retain - local MQTT)","topic":"","qos":"1","retain":"true","respTopic":"","contentType":"","userProps":"","correl":"","expiry":"","broker":"2ae2ce1a1d86db84","x":860,"y":720,"wires":[]},{"id":"d7bf30c2bca6789e","type":"function","z":"a21d7bcd93ad4405","name":"Build Discovery (once per session)","func":"const deviceId = 'mqtt_queue';\nconst channel = 'aws';\n\nconst stateTopic = 'buildingPULSE/' + deviceId + '/' + channel + '/state';\n\nconst entities = [\n    // How many messages are waiting to be sent to AWS\n    { key: 'queue_size', name: 'Queue Size', device_class: null, unit: '', state_class: 'measurement' },\n\n    // How many hours since the oldest unsent message was queued.\n    // If AWS has been unreachable for 6 hours, this reads 6.\n    // The single most useful \"is something wrong?\" indicator.\n    { key: 'oldest_pending_age_s', name: 'Oldest Pending Age', device_class: 'duration', unit: 's', state_class: 'measurement' },\n\n    // Ever-increasing count of rows successfully published to AWS.\n    // Useful for graphing throughput and spotting a stalled drain.\n    //{ key: 'sent_total', name: 'Sent Total', device_class: null, unit: '', state_class: 'total_increasing' },\n\n    // Count of rows that failed or timed out during publish.\n    // Should normally stay at zero.\n    //{ key: 'failed_count', name: 'Failed Count', device_class: null, unit: '', state_class: 'total_increasing' },\n\n    // ISO-8601 UTC timestamp of the last successful AWS publish.\n    // HA will show this as a human-readable \"X minutes ago\".\n    // Value must be an ISO string e.g. \"2024-01-15T10:30:00.000Z\"\n    //{ key: 'last_sent_ts', name: 'Last Sent', device_class: 'timestamp', unit: '', state_class: null },\n\n    // Total rows ever written to the DB (sent + unsent).\n    // Gives a lifetime message throughput figure.\n    //{ key: 'db_total', name: 'DB Total Rows', device_class: null, unit: '', state_class: 'total_increasing' },\n];\n\nconst msgs = entities.map(function (e) {\n    const config = {\n        name: e.name + '  ' + channel,\n        state_topic: stateTopic,\n        value_template: '{{ value_json.' + e.key + ' }}',\n        unique_id: deviceId + '_' + channel + '_' + e.key,\n        device: {\n            identifiers: [deviceId],\n            name: deviceId,\n            model: 'buildingPULSE MQTT QUEUE',\n            manufacturer: 'buildingPULSE'\n        }\n    };\n\n    // Only add optional fields when they have a value —\n    // sending null/empty to HA can cause validation warnings.\n    if (e.device_class) config.device_class = e.device_class;\n    if (e.state_class) config.state_class = e.state_class;\n    if (e.unit) config.unit_of_measurement = e.unit;\n\n    return {\n        topic: 'homeassistant/sensor/' + deviceId + '/' + channel + '_' + e.key + '/config',\n        payload: JSON.stringify(config),\n        retain: true,\n        qos: 1\n    };\n});\n\nreturn [msgs];","outputs":1,"timeout":"","noerr":0,"initialize":"","finalize":"","libs":[],"x":500,"y":720,"wires":[["60a09d6b9d90f95f","0ffde7f19fa07652"]]},{"id":"c0158bab4582cb2e","type":"function","z":"a21d7bcd93ad4405","name":"Build State Update","func":"// Publish the readings object to the shared state topic.\n// HA uses value_template to pull each field from this JSON.\nconst deviceId = \"mqtt_queue\";\nconst channel = \"aws\";\n\nmsg.topic   = 'buildingPULSE/' + deviceId + '/' + channel + '/state';\nmsg.payload = msg.payload[0];\nmsg.retain  = true;\nmsg.qos     = 1;\nreturn msg;","outputs":1,"timeout":"","noerr":0,"initialize":"","finalize":"","libs":[],"x":590,"y":840,"wires":[["10b778e6b70c6278"]]},{"id":"10b778e6b70c6278","type":"mqtt out","z":"a21d7bcd93ad4405","name":"HA State Update (local MQTT)","topic":"","qos":"1","retain":"false","respTopic":"","contentType":"","userProps":"","correl":"","expiry":"","broker":"2ae2ce1a1d86db84","x":850,"y":840,"wires":[]},{"id":"24c7c310e0589217","type":"inject","z":"a21d7bcd93ad4405","name":"Startup","props":[{"p":"payload"}],"repeat":"","crontab":"","once":true,"onceDelay":"5","topic":"","payload":"startup","payloadType":"str","x":140,"y":720,"wires":[["d7bf30c2bca6789e"]]},{"id":"05aefc92349e5123","type":"inject","z":"a21d7bcd93ad4405","name":"Every 15s","props":[{"p":"payload"}],"repeat":"15","crontab":"","once":true,"onceDelay":"15","topic":"","payload":"tick","payloadType":"str","x":150,"y":840,"wires":[["68debebd78768c2c","a0d53de874c6328f"]]},{"id":"68debebd78768c2c","type":"sqlite","z":"a21d7bcd93ad4405","mydb":"1579549ab2551290","sqlquery":"fixed","sql":"SELECT COUNT(*) AS queue_size\nFROM mqtt_queue\nWHERE sent = 0;","name":"unsent queue_size","x":350,"y":800,"wires":[["c0158bab4582cb2e"]]},{"id":"a0d53de874c6328f","type":"sqlite","z":"a21d7bcd93ad4405","mydb":"1579549ab2551290","sqlquery":"fixed","sql":"-- oldest_pending_age_s\n-- strftime('%s','now') gives current Unix seconds; ts is stored as ms\n-- Returns NULL when queue is empty - handle that in Node-RED\nSELECT ROUND(\n    (strftime('%s', 'now') * 1000.0 - MIN(ts))/1000.0, 1\n) AS oldest_pending_age_s\nFROM mqtt_queue\nWHERE sent = 0;","name":"oldest_pending_age_s","x":360,"y":860,"wires":[["c0158bab4582cb2e"]]},{"id":"0ffde7f19fa07652","type":"debug","z":"a21d7bcd93ad4405","name":"discovery","active":false,"tosidebar":true,"console":true,"tostatus":false,"complete":"true","targetType":"full","statusVal":"","statusType":"auto","x":730,"y":660,"wires":[]},{"id":"da29bed0d59d7a82","type":"comment","z":"a21d7bcd93ad4405","name":"Monitor: Show mqtt_queue stats in HA","info":"","x":190,"y":640,"wires":[]},{"id":"f63e7877fb17f4ba","type":"comment","z":"a21d7bcd93ad4405","name":"Fill: Insert MQTT messages into queue to send remote server","info":"","x":260,"y":180,"wires":[]},{"id":"3fd2ed288daf7d46","type":"comment","z":"a21d7bcd93ad4405","name":"Clean: delete messages that are too old > 24hrs","info":"","x":220,"y":1020,"wires":[]},{"id":"2e717393b93c0713","type":"inject","z":"a21d7bcd93ad4405","name":"Every 1hr","props":[{"p":"payload"}],"repeat":"3600","crontab":"","once":true,"onceDelay":"15","topic":"","payload":"tick","payloadType":"str","x":130,"y":1100,"wires":[["7f3018b6a28470ad"]]},{"id":"7f3018b6a28470ad","type":"sqlite","z":"a21d7bcd93ad4405","mydb":"1579549ab2551290","sqlquery":"fixed","sql":"DELETE FROM mqtt_queue\nWHERE ts < (strftime('%s', 'now') - 86400) * 1000;","name":"DELETE all messages > 24hrs old - these messages will be lost","x":470,"y":1100,"wires":[["20f2b217eb95d533"]]},{"id":"20f2b217eb95d533","type":"debug","z":"a21d7bcd93ad4405","name":"Write Delete message","active":true,"tosidebar":true,"console":true,"tostatus":false,"complete":"true","targetType":"full","statusVal":"","statusType":"auto","x":650,"y":1160,"wires":[]},{"id":"2ae2ce1a1d86db84","type":"mqtt-broker","name":"MQTT Local HA","broker":"core-mosquitto","port":1883,"clientid":"","autoConnect":true,"usetls":false,"protocolVersion":4,"keepalive":60,"cleansession":true,"autoUnsubscribe":true,"birthTopic":"","birthQos":"0","birthRetain":"false","birthPayload":"","birthMsg":{},"closeTopic":"","closeQos":"0","closeRetain":"false","closePayload":"","closeMsg":{},"willTopic":"","willQos":"0","willRetain":"false","willPayload":"","willMsg":{},"userProps":"","sessionExpiry":""},{"id":"1579549ab2551290","type":"sqlitedb","db":"/homeassistant/mqtt_queue.db","mode":"RWC"},{"id":"65ab4b77c78db112","type":"global-config","env":[],"modules":{"node-red-node-sqlite":"2.0.1"}}]
```

---

## Configuration

### MQTT broker

| Option | Description |
|---|---|
| **MQTT Broker Host** | Hostname or IP of the broker. Use `core-mosquitto` for the Mosquitto add-on on this host. |
| **MQTT Broker Port** | `1883` for plain-text, `8883` for TLS. |
| **Username / Password** | Leave both empty for anonymous access. |

### TLS

| Option | Description |
|---|---|
| **Enable TLS** | Encrypt the connection. Change the port to `8883` at the same time. |
| **CA Certificate File** | Path to a PEM file for the CA that signed the broker's certificate. Leave empty to use the system CA store (suitable for most public brokers). Example: `/config/certs/ca.pem` |
| **Verify CA Certificate** | Keep enabled in production. Disable only for local testing when you cannot supply a CA file. |
| **TLS Verification Hostname** | Set this when the broker is behind NAT or port-forwarding and `mqtt_host` is a local IP, but the broker's certificate is issued for its public name (e.g. `mqtt.example.com`). Leave empty to verify against `mqtt_host`. |

### Client certificate authentication

An alternative (or addition) to username/password: identifies this client
to the broker using an X.509 certificate.  Requires **Enable TLS: true**.

| Option | Description |
|---|---|
| **Client Certificate File** | Path to the client certificate PEM. Example: `/config/certs/client.crt` |
| **Client Key File** | Path to the matching private key PEM. Example: `/config/certs/client.key` |
| **Client Key Passphrase** | Only needed if the private key file is encrypted. |

Both certificate and key must be set together, or both left empty.

### SQLite database

| Option | Description |
|---|---|
| **SQLite Database Path** | Full path to the `.db` file containing the `mqtt_queue` table. `/config/mqtt_queue.db` keeps it alongside your other HA data and persists across restarts. |

### Logging

| Level | What you see |
|---|---|
| `error` | Failures only |
| `warn` | Failures and connection warnings |
| `info` | Normal operation (recommended) |
| `debug` | Every publish attempt and connection event |
| `trace` | Full message payload of every queued row |

---

## Troubleshooting

**Messages are not being forwarded**
- Check the add-on log for connection errors.
- Confirm the SQLite path is correct and the file is readable by the add-on.
- Verify the `mqtt_queue` table exists and contains rows with `sent = 0`.

**TLS certificate errors appear in the log**
- If connecting to a broker behind NAT, set **TLS Verification Hostname** to
  the broker's real public hostname.
- If using a private CA, set **CA Certificate File** to the path of your CA's
  PEM file.  Do not set **Verify CA** to false in production.

**Backoff / reconnect messages in the log**
- The add-on retries with exponential backoff (up to 60 s) on connection
  failure.  This is normal if the broker is temporarily unavailable.
