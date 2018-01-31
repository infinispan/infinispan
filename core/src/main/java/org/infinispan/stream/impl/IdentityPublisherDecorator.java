package org.infinispan.stream.impl;

import org.infinispan.commons.util.IntSet;
import org.infinispan.distribution.ch.ConsistentHash;
import org.reactivestreams.Publisher;

/**
 * PublishDecorator that just returns the publisher provided by the caller
 * @author wburns
 * @since 9.0
 */
class IdentityPublisherDecorator<S, R> implements PublisherDecorator<S> {
   private static final IdentityPublisherDecorator decorator = new IdentityPublisherDecorator();

   private IdentityPublisherDecorator() {
   }

   static <S, R> IdentityPublisherDecorator<S, R> getInstance() {
      return decorator;
   }

   @Override
   public Publisher<S> decorateRemote(ClusterStreamManager.RemoteIteratorPublisher<S> remotePublisher) {
      return remotePublisher;
   }

   @Override
   public Publisher<S> decorateLocal(ConsistentHash beginningCh, boolean onlyLocal, IntSet segmentsToFilter,
         Publisher<S> localPublisher) {
      return localPublisher;
   }
}
