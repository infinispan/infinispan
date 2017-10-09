package org.infinispan.query.dsl.embedded.impl;

/**
 * Apply a postprocessing transformation to the a query result generating an output value based on the input value.
 *
 * @author anistor@redhat.com
 * @since 9.0
 */
@FunctionalInterface
public interface ResultProcessor<In, Out> {

   Out process(In in);
}
