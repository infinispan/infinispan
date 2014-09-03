package org.infinispan.affinity;

import org.infinispan.Cache;
import org.infinispan.remoting.transport.Address;

import java.util.Collection;
import java.util.concurrent.Executor;

/**
 * @deprecated Extend from
 * {@link org.infinispan.affinity.impl.KeyAffinityServiceImpl}
 * instead. This class will be removed in the future.
 */
@Deprecated
public class KeyAffinityServiceImpl<K> extends org.infinispan.affinity.impl.KeyAffinityServiceImpl<K> {
   public KeyAffinityServiceImpl(Executor executor, Cache<? extends K, ?> cache, KeyGenerator<? extends K> keyGenerator,
         int bufferSize, Collection<Address> filter, boolean start) {
      super(executor, cache, keyGenerator, bufferSize, filter, start);
   }
}
