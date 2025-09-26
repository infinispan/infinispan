package org.infinispan.metrics.impl;

import org.infinispan.metrics.config.MicrometerMeterRegistryConfiguration;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;

/**
 * Concrete implementation of {@link MetricsRegistry}.
 * <p>
 * It uses {@link MeterRegistry} from micrometer. It can use the instance configured from
 * {@link MicrometerMeterRegistryConfiguration#meterRegistry()} or, if not configured, it instantiates
 * {@link PrometheusMeterRegistry}.
 */
public class PrometheusRegistry extends AbstractMetricsRegistry {

   ScrapeRegistry createScrapeRegistry(MeterRegistry registry) {
      boolean externalManaged = true;
      if (registry == null) {
         registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
         externalManaged = false;
      }
      return registry instanceof PrometheusMeterRegistry ?
            new PrometheusRegistrySimpleClient((PrometheusMeterRegistry) registry, externalManaged) :
            new NoScrapeRegistry(registry, externalManaged);
   }

   private static class PrometheusRegistrySimpleClient implements AbstractMetricsRegistry.ScrapeRegistry {

      final PrometheusMeterRegistry registry;
      final boolean externalManaged;

      PrometheusRegistrySimpleClient(PrometheusMeterRegistry registry, boolean externalManaged) {
         this.registry = registry;
         this.externalManaged = externalManaged;
      }

      @Override
      public boolean supportsScrape() {
         return true;
      }

      @Override
      public String scrape(String contentType) {
         return registry.scrape(contentType);
      }

      @Override
      public MeterRegistry registry() {
         return registry;
      }

      @Override
      public boolean externalManaged() {
         return externalManaged;
      }
   }
}
