/**
 * Micrometer-based metrics. All exported metrics are placed in VENDOR scope.
 * All io.micrometer.* references must be contained in MetricsCollector/MetricsCollectorFactory classes in a way
 * that allows the application to run without these dependencies.
 *
 * @api.private
 */
package org.infinispan.metrics.impl;
