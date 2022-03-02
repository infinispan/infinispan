/**
 * Micrometer based metrics. All exported metrics are placed in VENDOR scope.
 * All io.micrometer.* references must be contained in MetricsCollector/MetricsCollectorFactory classes in way
 * that allows the application to also be able to run without these dependencies.
 *
 * @private
 */
package org.infinispan.metrics.impl;
