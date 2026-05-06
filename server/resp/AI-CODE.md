# RESP Server Module Instructions

## Argument Parsing

When parsing numeric arguments from `byte[]` command arguments, use `ArgumentUtils` methods instead of creating intermediate `String` objects.

Use:
- `ArgumentUtils.toInt(byte[])` instead of `Integer.parseInt(new String(...))`
- `ArgumentUtils.toLong(byte[])` instead of `Long.parseLong(new String(...))`
- `ArgumentUtils.toDouble(byte[])` instead of `Double.parseDouble(new String(...))`

The `ArgumentUtils` class is in `org.infinispan.server.resp.commands.ArgumentUtils`.

When comparing `byte[]` arguments against known option strings (e.g. `WEIGHTS`, `WITHSCORES`, `NX`), use `RespUtil.isAsciiBytesEquals(byte[] expected, byte[] target)` instead of converting to `String` via `new String(arg).toUpperCase().equals(...)` or `new String(arg).equalsIgnoreCase(...)`. Define the expected value as a `private static final byte[]` constant:

```java
private static final byte[] WEIGHTS = "WEIGHTS".getBytes(StandardCharsets.US_ASCII);
// ...
if (RespUtil.isAsciiBytesEquals(WEIGHTS, arguments.get(i))) { ... }
```

This avoids `String` allocation and is case-insensitive (the expected value must be uppercase ASCII).

## Hashing

Probabilistic data structures (CuckooFilter, CountMinSketch) use the shared `MurmurHash64.hash(byte[] data, int seed)` utility in `org.infinispan.server.resp.commands.MurmurHash64`. Do not duplicate this implementation — this is different from `org.infinispan.commons.hash.MurmurHash3` used by BloomFilter.

## Implementing Commands

When implementing a RESP command:

- Always reference the official Redis documentation at `https://redis.io/docs/latest/commands/<command>` (e.g. `https://redis.io/docs/latest/commands/cms.merge/`) to ensure correct behavior, argument handling, and error responses.
- Commands must always return RESP3 responses.
- Connect to a locally-running Redis server on port 6379 to verify that commands declare the correct arity, firstkeypos, lastkeypos, ACL, etc.
- Ensure that error responses for syntax errors, invalid arguments, edge cases, etc. match the Redis ones.
- When the Redis documentation and the Redis implementation disagree, follow the implementation as the source of truth. In either case, if there is a mismatch in behavior between Infinispan and Redis, or between the Redis documentation and the Redis implementation, add a note to `documentation/src/main/resources/resp-command-notes.properties`.
- Always test both local and clustered environments.

## Command Structure

- Command classes extend `RespCommand` and implement `Resp3Command`.
- Commands are organized in subpackages under `commands/` by data type (e.g. `string/`, `hash/`, `list/`, `set/`, `sortedset/`, `bitmap/`, `json/`, `bloom/`, `cuckoo/`, `countmin/`, `geo/`, `pubsub/`, `connection/`, `generic/`, `cluster/`, `tx/`, `scripting/eval/`).
- Class names match the uppercase Redis command name (e.g. `APPEND.java`, `HGET.java`).
- For multi-keyword commands (e.g. `CMS.MERGE`), use a `FamilyCommand` base that delegates to subcommand classes.

### Constructor

The `RespCommand` constructor declares command metadata:

```java
super(arity, firstKeyPos, lastKeyPos, steps, aclMask);
// or for named commands:
super("CMS.MERGE", arity, firstKeyPos, lastKeyPos, steps, aclMask);
```

- **arity**: positive = fixed argument count (including command name), negative = minimum
- **firstKeyPos / lastKeyPos**: 1-indexed key positions (`0` = no keys, `-1` = unbounded)
- **steps**: increment between consecutive key positions (e.g. `2` for `MSET key val key val`)
- **aclMask**: bitwise OR of `AclCategory` values (e.g. `AclCategory.READ.mask() | AclCategory.STRING.mask() | AclCategory.FAST.mask()`)

### Registration

New commands must be registered in `Commands.java`.

### Execution Pattern

Commands implement `perform()` and return results via `handler.stageToReturn()`:

```java
@Override
public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                    ChannelHandlerContext ctx,
                                                    List<byte[]> arguments) {
    byte[] key = arguments.get(0);
    CompletionStage<byte[]> result = handler.cache().getAsync(key);
    return handler.stageToReturn(result, ctx, ResponseWriter.BULK_STRING_BYTES);
}
```

### Cache Access

- `handler.cache()` — `AdvancedCache<byte[], byte[]>` for string/generic commands
- `handler.typedCache(MediaType)` — typed cache for specialized data structures
- `handler.getListMultimap()` — for list commands
- `handler.getHashMapMultimap()` — for hash commands
- `handler.getEmbeddedSetCache()` — for set commands
- `handler.getSortedSeMultimap()` — for sorted set commands
- `handler.getJsonCache()` — for JSON commands

### Response Writing

Use static `ResponseWriter` consumers for common response types:

- `ResponseWriter.OK` — simple OK
- `ResponseWriter.BULK_STRING_BYTES` — byte array
- `ResponseWriter.INTEGER` — integer
- `ResponseWriter.DOUBLE` — double
- `ResponseWriter.ARRAY_BULK_STRING` — array of byte arrays

For custom responses, pass a lambda: `(result, writer) -> writer.integers(result)`.

Prefer the static consumers and `writer.array(collection, Resp3Type)` over manual `arrayStart`/`arrayEnd` loops. For example, use `ResponseWriter.ARRAY_INTEGER` instead of iterating and calling `w.integers()` per element.

### Error Handling

- Throw `NumberFormatException` for invalid numeric arguments (handled automatically as "value is not an integer or out of range").
- Use `handler.writer().customError(message)` for command-specific errors.
- Throw `RespCommandException` for custom error messages with the `-` prefix.
- `ClassCastException` is automatically mapped to `WRONGTYPE`.

## Testing

- Tests use **TestNG** (not JUnit 5) with `@Test(groups = "functional", testName = "server.resp.ClassName")`.
- The Redis client library for tests is **Lettuce** (`io.lettuce:lettuce-core`).
- Assertions use **AssertJ** (`assertThat(...)`, `assertThatThrownBy(...)`).
- Single-node tests extend `SingleNodeRespBaseTest`, are named `{Feature}Test`, and live in `src/test/java/org/infinispan/server/resp/`.
- Clustered tests extend `BaseMultipleRespTest`, are named `{Feature}ClusteredTest`, and live in `src/test/java/org/infinispan/server/resp/dist/`.
- Tests use the `factory()` method for parameterized runs (e.g. with and without authorization).
- For commands not natively supported by Lettuce, use `redis.dispatch()` with a custom `ProtocolKeyword`.
- Test utilities are in `org.infinispan.server.resp.test.RespTestingUtil`.
