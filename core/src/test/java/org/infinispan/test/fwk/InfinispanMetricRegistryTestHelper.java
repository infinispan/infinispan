package org.infinispan.test.fwk;

import org.eclipse.microprofile.metrics.MetricRegistry;
import org.infinispan.commands.module.TestGlobalConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.metrics.impl.InfinispanMetricRegistry;

import io.smallrye.metrics.MetricsRegistryImpl;

/**
 * Utility allowing a cache manager to have its own separate MetricRegistry in tests to avoid collisions due to the
 * microprofile metrics implementation using a single shared one per JVM.
 *
 * @author anistor@redhat.com
 * @since 10.1
 */
final class InfinispanMetricRegistryTestHelper {

   @Scope(Scopes.GLOBAL)
   static class TestInfinispanMetricRegistry extends InfinispanMetricRegistry {
      @Override
      protected MetricRegistry lookupRegistry() {
         // executed once for each cache manager, so each manager will have its own separate registry during tests
         return new MetricsRegistryImpl();
      }
   }

   /**
    * Replaces InfinispanMetricRegistry component with TestInfinispanMetricsRegistry to allow isolation between cache
    * managers.
    */
   static void replace(GlobalConfigurationBuilder gcb) {
      if (gcb.metrics().enabled()) {
         try {
            gcb.addModule(TestGlobalConfigurationBuilder.class)
               .testGlobalComponent(InfinispanMetricRegistry.class.getName(), new TestInfinispanMetricRegistry());
         } catch (LinkageError ignored) {
            // missing dependency
         }
      }
   }
}
