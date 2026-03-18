package org.infinispan.core.test.jupiter;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.infinispan.protostream.SerializationContextInitializer;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Declares a cross-site Infinispan topology for a test class.
 * <p>
 * Each {@link Site} is an independent cluster whose members see each other
 * via JGroups RELAY2. Sites are created once and shared across all test
 * methods; per-test caches are cleaned up automatically.
 * <p>
 * Example:
 * <pre>{@code
 * @InfinispanXSite({
 *    @Site(name = "LON", nodes = 2),
 *    @Site(name = "NYC", nodes = 2)
 * })
 * class MyXSiteTest {
 *
 *    @InfinispanResource
 *    XSiteContext ctx;
 *
 *    @Test
 *    void testSyncBackup() {
 *       var cache = ctx.createCache(c -> c
 *             .cacheMode(CacheMode.DIST_SYNC)
 *             .backups(BackupStrategy.SYNC));
 *
 *       cache.on("LON", 0).put("key", "value");
 *       assertThat(cache.on("NYC", 0).get("key")).isEqualTo("value");
 *    }
 * }
 * }</pre>
 *
 * @since 16.2
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Inherited
@ExtendWith(InfinispanXSiteExtension.class)
public @interface InfinispanXSite {

   /**
    * The sites in the topology. At least two sites are required.
    */
   Site[] value();

   /**
    * Whether to install a {@link org.infinispan.commons.time.ControlledTimeService}
    * on all cache managers across all sites.
    */
   boolean controlledTime() default false;

   /**
    * {@link SerializationContextInitializer} classes to register on all cache managers
    * across all sites. See {@link InfinispanCluster#serializationContext()} for details.
    */
   Class<? extends SerializationContextInitializer>[] serializationContext() default {};

   /**
    * Whether to enable global state persistence on all cache managers across all sites.
    * See {@link InfinispanCluster#globalState()} for details.
    */
   boolean globalState() default false;
}
