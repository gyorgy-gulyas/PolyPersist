# Identity & partition key

The contract every PolyPersist store implements.

## id — the logical identity
- `id` uniquely identifies an entity and is **globally unique** (a GUID by default).
- The **platform guarantees** this — the database does not have to enforce a global unique
  constraint, so each store is free to key optimally.
- This is d3i's `EntityId`.

## partitionKey — a required physical routing key
- `partitionKey` is **mandatory** (`CollectionCommon.CheckBeforeInsert/Update` rejects an
  empty one). It is *not* part of the logical identity; it tells partitioned stores **where**
  the entity lives.
- **Point access takes `(partitionKey, id)`** because distributed stores (Cassandra, Cosmos,
  DynamoDB, sharded MongoDB, partitioned SQL) require the partition key. This is the standard
  cloud-native pattern, not a compromise.

## Choosing the partition key (domain-decided)
- **Aggregate root → `partitionKey = id`** (its own partition; also the default when there is
  no natural partition, e.g. single-tenant / on-prem).
- **Child entity → `partitionKey = <parent aggregate id>`** for co-location
  (e.g. `ProjectAccess.PartitionKey => ProjectId`, `Auth.PartitionKey => accountId`).

Reference pattern (doctratis): a computed property mapping `PartitionKey` onto a real field,
so nothing is stored twice:
```csharp
string IEntity.PartitionKey { get => id;        set => id = value;        } // root
string IEntity.PartitionKey { get => ProjectId; set => ProjectId = value; } // child
```

## How each store keys (because id is globally unique)
| Store kind | Physical key | partitionKey role |
|---|---|---|
| Cassandra / Cosmos / sharded Mongo / partitioned SQL | `(partitionKey, id)` | required for routing |
| Blob (S3/Azure/GCS/FileSystem), unsharded Mongo, unpartitioned SQL | `id` | metadata / optional index; **not** needed in the object key |

Blobs gain nothing from a `partitionKey/id` object key (flat namespace), and a globally-unique
`id` makes the id-only key safe — so blobs key by `id` and keep `partitionKey` in metadata.
