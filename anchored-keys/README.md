= Scaling up without state transfer

The goal is to be able to add nodes to the cluster and make them own new entries, without also owning any of the old
entries.

Simplifying assumptions:

* Only one node is being added at a time.
* Single owner
* No transactions
* When scaling down data is just lost

The basic idea is that any time a key/value pair is inserted into the cache, the value is only written on the newest
member, which becomes the **anchor owner** of that key.

The cache is nominally a replicated cache, but all members except the anchor owner only store the key and the address of
the anchor.

It's important to note that clients don't have access to the anchor information, so they send operations to the *primary
owner*, **not** the *anchor owner*.
The primary owner then fetches the value from the anchor owner for reads or updates the value on the anchor owner for
writes.

== Implementation

Since we can't deterministically map keys to segments, we don't need a distributed cache.
Instead, we can use a `REPL_SYNC` cache, and the `ModuleLifecycle` implementation (`AnchoredKeysModule`) replaces/adds
components as needed.

The *anchor owner* information is stored in cache entries with value `null`, and the address in an instance of
`RemoteMetadata`.
In the `InvocationContext`, the values and metadata are replaced, so we extend `ReadCommittedEntry` with a location
field (`AnchoredReadCommittedEntry`).

For now, the distribution/triangle interceptor is replaced with `AnchoredDistributionInterceptor`, a custom interceptor
based on `NonTxDistributionInterceptor`,
which replaces the value/metadata in write commands sent to the backups with the anchor owner's address.

An additional `AnchoredFetchInterceptor` checks if the entry in the context is a `RemoteMetadata` entry and fetches the
existing values from the anchor owner if needed.
`AnchoredFetchInterceptor` is also responsible for computing the new anchor owner for an entry if the key does not yet
exist in the cache or if the current anchor owner has left the cluster.

The state provider is also replaced with an `AnchoredStateProvider`, sending the anchor owner's address to the joiners
instead of the actual values.

=== Listeners
`AnchoredReadCommittedEntry` also allows the primary owner to invoke the cluster listeners with the correct value and
only store the anchor owner address in the `DataContainer`.
This means cluster listeners and client listeners receive the correct notifications.

Note: Non-clustered embedded listeners currently receive notifications on all the nodes, not just the node where the
value is stored. This will change in the future.

== Configuration

Configuration means a custom element `anchored-keys`
with a single attribute `enabled`.

```xml

<replicated-cache name="name">
    <anchored-keys enabled="true"/>
</replicated-cache>
```

Despite the title of the document, state transfer should **not** be disabled.
The cache needs to receive the keys and their anchor addresses,
but it also works (slightly slower) while state transfer is in progress,
so `await-initial-transfer` should be disabled:

```xml

<replicated-cache name="name">
    <anchored-keys enabled="true"/>
    <state-transfer chunk-size="5000" await-initial-transfer="false"/>
</replicated-cache>
```

== Performance considerations

=== Client/Server Latency

The client always contacts the primary owner, so any read has a `(N-1)/N` probability of requiring a unicast RPC from
the primary to the anchor owner.

Writes require the primary to send the value to one node and the anchor address to all the other nodes, which is
currently done with `N-1` unicast RPCs.

In theory we could send in parallel one unicast RPC for the value and one multicast RPC for the address, but that would
need additional logic to ignore the address on the anchor owner and with TCP multicast RPCs are implemented as parallel
unicasts anyway.

=== Memory overhead

The anchor cache contains copies of all the keys and their locations, plus the overhead of the cache itself.

The overhead is lowest for off-heap storage: 21 bytes in the entry itself plus 8 bytes in the table, assuming no
eviction or expiration.
The location is another 20 bytes, assuming we keep the serialized owner's address.

Note: We could reduce the location size to <= 8 bytes by encoding the location as an integer.

Addresses are interned, so an address already uses only 4 bytes with OBJECT storage and `-XX:+UseCompressedOops`.
But the overhead of the ConcurrentHashMap-based on-heap cache is much bigger, at least 32 bytes from CHM.Node, 24 bytes
from ImmortalCacheEntry, and 4 bytes in the table.

=== State transfer

State transfer does not transfer the actual values, but it still needs to transfer all the keys and the anchor owner
information.

Assuming that the values are much bigger compared to the keys, the anchor cache's state transfer should also be much
faster compared to the state transfer of a distributed cache of a similar size.

The initial state transfer should not block a joiner from starting, because the joiner can ask an existing node for the
location.
