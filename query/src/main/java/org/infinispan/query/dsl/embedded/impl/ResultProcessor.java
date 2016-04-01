package org.infinispan.query.dsl.embedded.impl;

/**
 * @author anistor@redhat.com
 * @since 9.0
 */
@FunctionalInterface
public interface ResultProcessor<In, Out> {

   Out process(In result);
}
