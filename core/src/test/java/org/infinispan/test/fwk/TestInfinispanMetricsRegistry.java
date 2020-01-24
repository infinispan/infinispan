package org.infinispan.test.fwk;

import org.eclipse.microprofile.metrics.MetricRegistry;
import org.infinispan.commands.module.TestGlobalConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.metrics.impl.InfinispanMetricsRegistry;

import io.smallrye.metrics.MetricsRegistryImpl;

/**
 * Allows each cache manager to have its own separate MetricRegistry (the eclipse microprofile metrics standard uses a
 * shared one per JVM).
 *
 * @author anistor@redhat.com
 * @since 10.1
 */
final class TestInfinispanMetricsRegistry {

   static void replace(GlobalConfigurationBuilder gcb) {
      try {
         gcb.addModule(TestGlobalConfigurationBuilder.class)
            .testGlobalComponent(InfinispanMetricsRegistry.class.getName(), new InfinispanMetricsRegistry() {
               @Override
               protected MetricRegistry makeRegistry() {
                  return new MetricsRegistryImpl();
               }
            });
      } catch (NoClassDefFoundError e) {
         // missing dependency?
      }
   }
}
