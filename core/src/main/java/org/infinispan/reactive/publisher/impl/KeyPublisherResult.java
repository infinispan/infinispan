package org.infinispan.reactive.publisher.impl;

import java.util.Set;

import org.infinispan.commons.util.IntSet;

/**
 * A PublisherResult that was performed due to included keys. Note that this response is only ever created on the
 * originator node. This is because we can't have a partial response with key based publishers. Either all results
 * are returned or the node crashes or has an exception.
 * @author wburns
 * @since 10.0
 */
public class KeyPublisherResult<K, R> implements PublisherResult<R> {
   private final Set<K> suspectedKeys;

   public KeyPublisherResult(Set<K> suspectedKeys) {
      this.suspectedKeys = suspectedKeys;
   }

   @Override
   public IntSet getSuspectedSegments() {
      return null;
   }

   @Override
   public Set<K> getSuspectedKeys() {
      return suspectedKeys;
   }

   @Override
   public R getResult() {
      return null;
   }

   @Override
   public String toString() {
      return "KeyPublisherResult{" +
            ", suspectedKeys=" + suspectedKeys +
            '}';
   }
}
