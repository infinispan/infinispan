package org.infinispan.stream.impl;

import org.infinispan.commons.util.IntSet;
import org.infinispan.distribution.ch.ConsistentHash;
import org.reactivestreams.Publisher;

/**
 * Decorator that decorates publishers based on if it is local or remote to handle segment completions
 * @author wburns
 * @since 9.0
 */
interface PublisherDecorator<S> {
   /**
    * Invoked for each remote publisher to provide additional functionality
    * @param remotePublisher the provided remote publisher
    * @return the resulting publisher (usually wrapped in some way)
    */
   Publisher<S> decorateRemote(ClusterStreamManager.RemoteIteratorPublisher<S> remotePublisher);

   /**
    * Invoked for a local publisher, which only completes segments if the consistent hash after completion is the same
    * as the one provided
    * @param beginningCh the consistent has to test against
    * @param onlyLocal whether this publisher is only done locally (that is there are no other remote publishers)
    * @param segmentsToFilter the segments to use for this invocation
    * @param localPublisher the internal local publisher
    * @return the resulting publisher (usually wrapped in some way)
    */
   Publisher<S> decorateLocal(ConsistentHash beginningCh, boolean onlyLocal, IntSet segmentsToFilter,
         Publisher<S> localPublisher);
}
