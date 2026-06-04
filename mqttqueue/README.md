# MQTT Queue Forwarder

A Home Assistant add-on that reliably forwards messages from a local SQLite
queue to an MQTT broker using QOS-2 (exactly-once delivery).

## What is this for?

If you have an application running on the same host that needs to publish MQTT
messages but cannot always reach the broker — due to network interruptions,
broker restarts, or startup ordering — this add-on acts as a reliable buffer.

Your application (perhaps node-red) writes rows into a local SQLite database and returns
immediately.  This add-on handles delivery, retries, and confirmation in the
background, so your application never blocks waiting for the broker.

## Features

- **QOS-2 delivery** — every message is confirmed end-to-end before being
  marked sent
- **Pipelined publishing** — up to 20 messages in-flight simultaneously for
  high throughput
- **Automatic reconnect** — exponential backoff with configurable timeout
- **TLS support** — including client certificate authentication and hostname
  override for brokers behind NAT or port-forwarding
- **Graceful shutdown** — in-flight messages are drained before the add-on
  stops

## Requirements

A SQLite database accessible to the add-on (e.g. under `/config/`) containing
an `mqtt_queue` table.  See the **Documentation** tab for the expected schema.

## Support

For issues and questions, visit the [project repository][url].

[url]: https://github.com/yourorg/mqtt_forwarder
