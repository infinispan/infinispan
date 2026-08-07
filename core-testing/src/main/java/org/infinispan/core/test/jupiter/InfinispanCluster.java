package org.infinispan.core.test.jupiter;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.infinispan.protostream.SerializationContextInitializer;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Declares an embedded Infinispan cluster for a test class.
 * <p>
 * The cluster is created once and shared across all test methods in the class.
 * Each test method gets its own dedicated caches, so tests are isolated even when
 * sharing the same cache managers.
 * <p>
 * Example usage:
 * <pre>{@code
 * @InfinispanCluster(numNodes = 3)
 * class MyClusteredTest {
 *
 *    @InfinispanResource
 *    InfinispanContext ctx;
 *
 *    @Test
 *    void testDistribution() {
 *       var cache = ctx.createCache(b -> b.clustering().cacheMode(CacheMode.DIST_SYNC));
 *       cache.on(0).put("key", "value");
 *       assertThat(cache.on(1).get("key")).isEqualTo("value");
 *    }
 * }
 * }</pre>
 *
 * @since 16.2
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Inherited
@ExtendWith(InfinispanExtension.class)
public @interface InfinispanCluster {

   /**
    * Number of cache manager nodes in the cluster. Use 1 for non-clustered tests.
    */
   int numNodes() default 1;

   /**
    * Whether to install a {@link org.infinispan.commons.time.ControlledTimeService}
    * on all cache managers. This allows manual time advancement for testing
    * expiration, timeouts, and other time-dependent features.
    */
   boolean controlledTime() default false;

   /**
    * JGroups transport stack to use. Defaults to the test UDP stack.
    */
   String transportStack() default "test-udp";

   /**
    * Path to an Infinispan configuration file (XML, JSON, or YAML) to use as a
    * starting point. The file is resolved from the classpath or filesystem.
    * <p>
    * The transport configuration from the file is replaced by the test harness
    * transport to ensure cluster isolation. Named caches defined in the file
    * are available via {@link InfinispanContext#manager(int)}.
    * <p>
    * Example:
    * <pre>{@code
    * @InfinispanCluster(numNodes = 2, config = "my-test-config.xml")
    * class MyTest { ... }
    * }</pre>
    */
   String config() default "";

   /**
    * {@link SerializationContextInitializer} classes to register on all cache managers.
    * <p>
    * Each class is resolved by looking for a static {@code INSTANCE} field first,
    * then falling back to a no-arg constructor.
    * <p>
    * Example:
    * <pre>{@code
    * @ProtoSchema(includeClasses = {MyEntity.class}, schemaFileName = "test.proto",
    *              schemaFilePath = "/", schemaPackageName = "test")
    * interface MyTestSCI extends SerializationContextInitializer {
    *    MyTestSCI INSTANCE = new MyTestSCIImpl();
    * }
    *
    * @InfinispanCluster(numNodes = 2, serializationContext = MyTestSCI.class)
    * class MyTest { ... }
    * }</pre>
    */
   Class<? extends SerializationContextInitializer>[] serializationContext() default {};

   /**
    * Whether to enable global state persistence on all cache managers.
    * <p>
    * When enabled, each node gets an isolated persistent location under a
    * temporary directory. This allows testing restart scenarios where a node
    * stops and restarts, recovering its state from disk.
    * <p>
    * Use {@link InfinispanContext#restart(int)} to stop a node and recreate it
    * with the same persistent location, simulating a node restart with state recovery.
    * <p>
    * Example:
    * <pre>{@code
    * @InfinispanCluster(numNodes = 2, globalState = true)
    * class MyRestartTest {
    *    @InfinispanResource
    *    InfinispanContext ctx;
    *
    *    @Test
    *    void testRestartWithStateRecovery() {
    *       var cache = ctx.createCache(b -> b.clustering().cacheMode(CacheMode.DIST_SYNC));
    *       cache.on(0).put("key", "value");
    *
    *       ctx.restart(0);  // stop + restart node 0, recovering state
    *
    *       assertThat(cache.on(0).get("key")).isEqualTo("value");
    *    }
    * }
    * }</pre>
    */
   boolean globalState() default false;
}
