package org.infinispan.affinity;

import org.infinispan.lifecycle.Lifecycle;
import org.infinispan.remoting.transport.Address;

/**
 * Defines a service that generates keys to be mapped to specific nodes in a distributed(vs. replicated) cluster.
 * The service is instantiated through through one of the factory methods from {@link org.infinispan.affinity.KeyAffinityServiceFactory}.
 * <p/>
 * Sample usage:
 * <p/>
 * <pre><code>
 *    Cache&lt;String, Long&gt; cache = getDistributedCache();
 *    KeyAffinityService&lt;String&gt; service = KeyAffinityServiceFactory.newKeyAffinityService(cache, 100);
 *    ...
 *    String sessionId = sessionObject.getId();
 *    String newCollocatedSession = service.getCollocatedKey(sessionId);
 *
 *    //this will reside on the same node in the cluster
 *    cache.put(newCollocatedSession, someInfo);
 * </code></pre>
 * <p/>
 * Uniqueness: the service does not guarantee that the generated keys are unique. It relies on an
 * {@link org.infinispan.affinity.KeyGenerator} for obtaining and distributing the generated keys. If key uniqueness is
 * needed that should be enforced in the generator.
 * <p/>
 * The service might also drop key generated through the {@link org.infinispan.affinity.KeyGenerator}.
 *
 * @see org.infinispan.affinity.KeyAffinityServiceFactory
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public interface KeyAffinityService<K> extends Lifecycle {

   /**
    * Returns a key that will be distributed on the cluster node identified by address.
    * @param address identifying the cluster node.
    * @return a key object
    * @throws IllegalStateException if the service has not been started or it is shutdown
    */
   K getKeyForAddress(Address address);

   /**
    * Returns a key that will be distributed on the same node as the supplied key.
    * @param otherKey the key for which we need a collocation
    * @return a key object
    * @throws IllegalStateException if the service has not been started or it is shutdown
    */
   K getCollocatedKey(K otherKey);

   /**
    * Checks weather or not the service is started.
    */
   boolean isStarted();
}
