# UUIDv7 timestamps

Charisma uses an ordered UUID variant (UUIDv7) when you need time-sortable identifiers. UUIDv7 combines millisecond-resolution timestamp information with randomness so generated values are both globally unique and roughly ordered by creation time.

Why UUIDv7

- Improved insertion order locality for index performance compared with fully-random UUIDv4.
- Retains global uniqueness and randomness to avoid collisions in distributed systems.

Schema usage

Example `schema.charisma` field:

model Post {
id String @id @default(uuid_v7())
text String
}

- Use `@default(uuid_v7())` to have the generator emit a default expression that creates a UUIDv7 value in the database.

Runtime notes

- The runtime contains a helper type at `src/Charisma.Runtime/UuidV7Timestamp.cs` that implements utilities for extraction and conversion; projects can inspect or convert timestamps when required.
- Database functions and syntax vary by provider; ensure your Postgres extension or helper function implementing `uuid_v7()` is available (or use the generator/runtime-provided function where supported).

When to prefer UUIDv7

- Use when write-heavy workloads benefit from roughly ordered primary keys (reduced index fragmentation).
- If you require strict monotonic sequence numbers, use a separate sequence or timestamp column instead.
