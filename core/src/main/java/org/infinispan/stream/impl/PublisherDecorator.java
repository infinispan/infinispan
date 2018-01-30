package org.infinispan.stream.impl;

import org.infinispan.commons.util.IntSet;
import org.infinispan.distribution.ch.ConsistentHash;
import org.reactivestreams.Publisher;

/**
 * @author wburns
 * @since 9.0
 */
interface PublisherDecorator<S> {
   Publisher<S> decorateRemote(ClusterStreamManager.RemoteIteratorPublisher<S> remotePublisher);

   Publisher<S> decorateLocal(ConsistentHash beginningCh, boolean onlyLocal, IntSet segmentsToFilter,
         Publisher<S> localPublisher);
}
