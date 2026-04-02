package org.infinispan.metrics.impl;

import java.util.Objects;

import io.micrometer.core.instrument.MeterRegistry;

/**
 * A concrete implementation of {@link MetricsRegistry} that accepts any Micrometer {@link MeterRegistry}
 * implementation.
 * <p>
 * This registry implementation provides flexibility by allowing integration with any Micrometer-compatible meter
 * registry. The provided registry is treated as externally managed, meaning its lifecycle (creation and closure) is
 * handled outside of Infinispan.
 * <p>
 * <strong>Note:</strong> This implementation does not support metric scraping. The {@link #supportScrape()} method
 * will return {@code false}, and {@link #scrape(String)} will return {@code null}.
 *
 * @see AbstractMetricsRegistry
 * @see MeterRegistry
 */
public class GenericMetricRegistry extends AbstractMetricsRegistry {
   @Override
   ScrapeRegistry createScrapeRegistry(MeterRegistry registry) {
      Objects.requireNonNull(registry);
      return new NoScrapeRegistry(registry, true);
   }
}
