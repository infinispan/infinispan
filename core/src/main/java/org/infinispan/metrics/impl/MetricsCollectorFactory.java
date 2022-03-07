package org.infinispan.metrics.impl;

import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.AutoInstantiableFactory;
import org.infinispan.factories.ComponentFactory;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.config.MeterFilter;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;

/**
 * Produces instances of {@link MetricsCollector}. MetricsCollector is optional,
 * based on the presence of Micrometer in classpath and the enabling of metrics in config.
 *
 * @author anistor@redhat.com
 * @author Fabio Massimo Ercoli
 * @since 10.1
 */
@DefaultFactoryFor(classes = MetricsCollector.class)
@Scope(Scopes.GLOBAL)
public final class MetricsCollectorFactory implements ComponentFactory, AutoInstantiableFactory {

   private static final Log log = LogFactory.getLog(MetricsCollectorFactory.class);

   @Inject
   GlobalConfiguration globalConfig;

   @Override
   public Object construct(String componentName) {
      if (!globalConfig.metrics().enabled()) {
         return null;
      }

      // try cautiously
      try {
         return create();
      } catch (Throwable e) {
         // missing dependency
         log.debug("Micrometer metrics are not available due to missing classpath dependencies.", e);
         return null;
      }
   }

   private MetricsCollector create() {
      PrometheusMeterRegistry baseRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
      baseRegistry.config().meterFilter(new BaseFilter());

      new BaseAdditionalMetrics().bindTo(baseRegistry);

      PrometheusMeterRegistry vendorRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
      vendorRegistry.config().meterFilter(new VendorFilter());

      new VendorAdditionalMetrics().bindTo(vendorRegistry);

      return new MetricsCollector(baseRegistry, vendorRegistry);
   }

   private static class BaseFilter implements MeterFilter {
      private static final String PREFIX = "base.";

      @Override
      public Meter.Id map(Meter.Id id) {
         return id.withName(PREFIX + id.getName());
      }
   }

   private static class VendorFilter implements MeterFilter {
      private static final String PREFIX = "vendor.";

      @Override
      public Meter.Id map(Meter.Id id) {
         return id.withName(PREFIX + id.getName());
      }
   }
}
