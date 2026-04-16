# Testing Instructions

## Test Frameworks by Module

Infinispan uses two test frameworks depending on the module:

| Module Type                | Framework         | Base Classes                                          | Test Naming |
|----------------------------|-------------------|-------------------------------------------------------|-------------|
| Core / embedded modules    | TestNG            | `SingleCacheManagerTest`, `MultipleCacheManagersTest` | `*Test`     |
| Server / integration tests | JUnit 5 (Jupiter) | `InfinispanServerExtension` via `@RegisterExtension`  | `*IT`       |

Both frameworks are bridged via `org.junit.support:testng-engine` so Maven can run everything through the JUnit Platform.

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

- **`CONTAINER`** — runs server in Docker (default for CI)
- **`FORKED`** — runs server in a forked JVM process

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
