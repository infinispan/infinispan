/**
 * Eclipse microprofile based metrics implementation. All exported metrics are placed in VENDOR scope. Implementation is
 * based on smallrye-metrics. All org.eclipse.microprofile.metrics/config and io.smallrye.metrics references must be
 * contained in MetricsCollector/MetricsCollectorFactory classes in way that allows the application to also be able to
 * run without these dependencies.
 *
 * @api.private
 */
package org.infinispan.metrics.impl;
