package org.infinispan.reactive.publisher.impl;

import java.util.Set;

import org.infinispan.commons.util.IntSet;

/**
 * A result from a publisher. It may or may not contain a result. Also depending on the type of publisher operation
 * it may or may not contain either suspected segments or suspected keys, but never both.
 * @author wburns
 * @since 10.0
 */
public interface PublisherResult<R> {
   IntSet getSuspectedSegments();

   Set<?> getSuspectedKeys();

   R getResult();
}
