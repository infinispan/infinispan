package org.infinispan.test.fwk;

import org.infinispan.commands.module.TestGlobalConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.metrics.impl.InfinispanMetricRegistry;

import io.smallrye.metrics.MetricsRegistryImpl;

/**
 * Utility allowing a cache manager to have its own separate MetricRegistry in tests to avoid collisions due to the
 * microprofile metrics implementation using a single shared registry per JVM.
 *
 * @author anistor@redhat.com
 * @since 10.1
 */
final class InfinispanMetricRegistryTestHelper {

   @Scope(Scopes.GLOBAL)
   static class TestInfinispanMetricRegistry extends InfinispanMetricRegistry {

      TestInfinispanMetricRegistry() {
         // executed once for each cache manager
         super(new MetricsRegistryImpl());
      }
   }

   /**
    * Replaces InfinispanMetricRegistry component with one that uses a private MetricRegistry to allow isolation between
    * cache managers and avoid unintended metric collisions.
    */
   static void replace(GlobalConfigurationBuilder gcb) {
      if (gcb.metrics().enabled()) {
         try {
            gcb.addModule(TestGlobalConfigurationBuilder.class)
               .testGlobalComponent(InfinispanMetricRegistry.class.getName(), new TestInfinispanMetricRegistry());
         } catch (LinkageError ignored) {
            // missing metrics related dependency
         }
      }
   }
}
