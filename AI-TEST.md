# Testing Instructions

## Test Frameworks by Module

Infinispan uses two test frameworks depending on the module:

| Module Type                | Framework         | Base Classes                                          | Test Naming |
|----------------------------|-------------------|-------------------------------------------------------|-------------|
| Core / embedded modules    | TestNG            | `SingleCacheManagerTest`, `MultipleCacheManagersTest` | `*Test`     |
| Server / integration tests | JUnit 5 (Jupiter) | `InfinispanServerExtension` via `@RegisterExtension`  | `*IT`       |

Both frameworks are bridged via `org.junit.support:testng-engine` so Maven can run everything through the JUnit Platform.

## Mandatory: Clustered and Marshalling Test Coverage

Every feature or fix that interacts with the cache MUST be evaluated for the following test coverage. Not every change needs all levels, but the evaluation is mandatory.

### Clustered tests
If the feature involves cache operations, commands, or functions that are sent across nodes, you MUST have clustered tests (`MultipleCacheManagersTest` with 2+ nodes). Single-node tests alone do NOT validate that marshalling, replication, and backup operations work correctly. Use `CacheMode.DIST_SYNC` or `CacheMode.REPL_SYNC` to catch clustering issues.

### Marshalling tests
Any new `@ProtoTypeId` class, command, or marshallable function MUST be tested for correct serialization/deserialization. This includes:
- Functions passed to `cache.compute()`, `cache.merge()`, or CacheStream operations
- New commands or command parameters
- Classes carrying heterogeneous data (e.g. `Map<String, Object>` via `MarshallableMap`)

### Remote/server tests
Features exposed via Hot Rod or REST MUST have server integration tests (`server/tests/`) that verify the end-to-end path through the protocol layer. The test hierarchy for query features is: `QueryStringTest` (embedded) -> `RemoteQueryStringTest` (remote) -> `HotRodCacheQueries` (server/native).

## Writing Core (Embedded) Tests

### Base Classes

Choose the right base class in `org.infinispan.test`:

- **`SingleCacheManagerTest`** — tests that need one embedded cache manager. Override `createCacheManager()` to configure it.
- **`MultipleCacheManagersTest`** — tests that need a cluster. Override `createCacheManagers()` to set up multiple nodes. Supports `@Factory` for parameterized runs across cache modes, transaction modes, etc.
- **`AbstractInfinispanTest`** — low-level base if you don't need a cache manager at all.

### Example: Single Cache Manager Test

```java
@Test(groups = "functional", testName = "MyFeatureTest")
public class MyFeatureTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      return TestCacheManagerFactory.createCacheManager(
         TestCacheManagerFactory.getDefaultCacheConfiguration(false)
      );
   }

   public void testSomething() {
      cache().put("key", "value");
      assertEquals("value", cache().get("key"));
   }
}
```

### Example: Clustered Test

```java
@Test(groups = "functional", testName = "MyClusteredTest")
public class MyClusteredTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cfg = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      createClusteredCaches(2, cfg);
   }

   public void testReplication() {
      cache(0).put("key", "value");
      assertEquals("value", cache(1).get("key"));
   }
}
```

### TestNG Groups

Always annotate tests with a group. Common groups:
- `"functional"` — standard functional tests (default CI)
- `"unit"` — unit tests (default CI)
- `"stress"` — stress/performance tests (excluded from default CI, run with `-Ptest-stress`)
- `"unstable"` — known flaky tests (excluded from default CI)
- `"xsite"` — cross-site replication tests

### Key Utilities

- **`TestCacheManagerFactory`** — create pre-configured cache managers for tests
- **`TestingUtil`** — 300+ utility methods: `extractComponent()`, `killCacheManagers()`, `waitForRehashToComplete()`, etc.
- **`eventually()`** — poll-based assertions with timeout (from `AbstractInfinispanTest`)
- **`fork()`** — run tasks in test executor for concurrent test scenarios

### Cleanup

- Default: cleanup after each test method
- Use `@CleanupAfterTest` on the class to clean up only after the entire test class completes
- Use `@CleanupAfterMethod` to make method-level cleanup explicit

## Writing Server (Integration) Tests

Server tests live in `server/tests/` and use JUnit 5 with the Infinispan server test driver.

### Server Test Driver

```java
public class MyServerIT {

   @RegisterExtension
   static InfinispanServerExtension SERVER = InfinispanServerExtensionBuilder
       .config("infinispan.xml")
       .numServers(2)
       .build();

   @Test
   public void testCacheViaHotRod() {
      RemoteCache<String, String> cache = SERVER.hotrod().create();
      cache.put("key", "value");
      assertEquals("value", cache.get("key"));
   }
}
```

### Multi-Protocol Client Access

The `TestClient` object provides access to all protocols:
- `SERVER.hotrod()` — Hot Rod client
- `SERVER.rest()` — REST client
- `SERVER.resp()` — RESP (Redis) client
- `SERVER.memcached()` — Memcached client
- `SERVER.jmx()` — JMX client

### Server Run Modes

- **`CONTAINER`** — runs server in Docker or Podman (default for CI; enable Podman via `-Ppodman`, defaults to Docker)
- **`FORKED`** — runs server in a forked JVM process

#### Running with rootless Podman

The `CONTAINER` driver connects to servers using the container's bridge IP (see `InfinispanGenericContainer.getIpAddress()`), not mapped ports. With rootless Podman the bridge network lives in a separate network namespace, so the host cannot reach those IPs and tests fail with `ConnectTimeoutException` (e.g. `/10.89.0.2:11222`).

Use `bin/run-in-podman.sh` to run the whole Maven build inside Podman's rootless network namespace. It prefixes the command with the needed setup and adds `-Ppodman` unless you already passed it:

```bash
bin/run-in-podman.sh mvn verify -pl server/tests -Dit.test=MyTestIT
```

The script is equivalent to:

```bash
podman unshare --rootless-netns mvn -Dmaven.repo.local=$HOME/.m2/repository -Duser.home=$HOME -Ppodman verify -pl server/tests -Dit.test=MyTestIT
```

- `-Ppodman` sets `DOCKER_HOST` and `TESTCONTAINERS_RYUK_DISABLED` for Testcontainers.
- `-Dmaven.repo.local` and `-Duser.home` are required because inside `podman unshare` Maven runs as uid 0 and would otherwise look for `/root/.m2`. The script sets `maven.repo.local` to `~/.m2/repository` unless you pass your own.
- The Podman API socket must be running (`systemctl --user enable --now podman.socket`).
- Wrap the `mvn` process, **not** the forked test JVM (e.g. through a custom `-Djvm=` script). The forked JVM reports back to Maven over a localhost socket, which does not exist across namespaces, so the build hangs.

### Server Test Configuration

Place server XML configurations in `server/tests/src/test/resources/configuration/`.

### JUnit Categories

For JUnit 5 tests, use category marker interfaces from `org.infinispan.testing.categories`:
- `Smoke` — smoke tests
- `Stress` — stress tests
- `Unstable` — known flaky tests
- `Profiling` — profiling tests

## Debugging Container Tests

To debug a test running inside a container:

1. Start your IDE debugger in **listening/server mode** on port **5005**
2. Set breakpoints
3. Run the test with the debug system property:

```bash
mvn verify -pl server/tests -Dit.test=MyTestIT -Dorg.infinispan.test.server.container.debug=0
```

The value (`0`) is the index of the container to debug (0 for the first server).

### Other Container Properties

- `org.infinispan.test.server.container.timeoutSeconds` — container startup timeout
- `org.infinispan.test.server.container.memory` — container memory limit
- `org.infinispan.test.server.container.ulimit` — resource limits

## Running Tests

```bash
# Run all tests in a module
mvn verify -pl core -Dcheckstyle.skip

# Run a single test class
mvn verify -pl core -Dtest=MyFeatureTest -Dcheckstyle.skip

# Run a single test method
mvn verify -pl core -Dtest=MyFeatureTest#testSomething -Dcheckstyle.skip

# Run server integration tests
mvn verify -pl server/tests -Dit.test=MyServerIT -Dcheckstyle.skip

# Run only smoke tests
mvn verify -pl core -Psmoke -Dcheckstyle.skip

# Run stress tests (excluded by default)
mvn verify -pl core -Ptest-stress -Dcheckstyle.skip
```
