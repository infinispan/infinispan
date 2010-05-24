package org.infinispan.affinity;

import org.infinispan.remoting.transport.Address;

import java.util.concurrent.Executor;

/**
 *
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public interface KeyAffinityService<K> {
   
   public K getKeyForAddress(Address address);

   public K getCollocatedKey(K otherKey);   
}
