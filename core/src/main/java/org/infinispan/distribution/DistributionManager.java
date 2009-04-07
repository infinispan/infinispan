package org.infinispan.distribution;

import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;

/**
 * A component that manages the distribution of elements across a cache cluster
 *
 * @author Manik Surtani
 * @since 4.0
 */
@Scope(Scopes.GLOBAL)
public interface DistributionManager {

   void rehash();

   ConsistentHash getConsistentHash();

   boolean isLocal(Object key);

   Object retrieveFromRemote(Object key) throws Exception;
}

