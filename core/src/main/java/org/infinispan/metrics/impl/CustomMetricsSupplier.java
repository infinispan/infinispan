package org.infinispan.metrics.impl;

import static org.infinispan.factories.impl.MBeanMetadata.AttributeMetadata;

import java.util.Collection;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;

import org.infinispan.jmx.annotations.ManagedAttribute;

/**
 * Interface to implement if the class want to register more metrics to Micrometer than the methods annotated with
 * {@link ManagedAttribute}.
 * <p>
 * The main goal is to allow some dynamic metrics (i.e. metrics that depends on some configuration). As an example, the
 * Cross-Site response time for each configured site.
 * <p>
 * {@link MetricUtils#createGauge(String, String, Function, Map)} or
 * {@link MetricUtils#createTimer(String, String, BiConsumer, java.util.Map)} can be used to create this custom
 * metrics.
 *
 * @author Pedro Ruivo
 * @since 13.0
 */
public interface CustomMetricsSupplier {

   /**
    * Extra metrics to be registered.
    * <p>
    * These can be dynamic metrics that cannot use {@link ManagedAttribute} annotation. Extra tags can be set in
    * {@link AttributeMetadata}.
    *
    * @param nameAsTag True if the cache name or any other identifier must be set as Tags instead of metric name.
    * @return A list of {@link AttributeMetadata} to be registered.
    */
   Collection<AttributeMetadata> getCustomMetrics(boolean nameAsTag);

}
