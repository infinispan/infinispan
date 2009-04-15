package org.infinispan.distribution;

import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.remoting.transport.Address;

import java.util.List;
import java.util.Map;

/**
 * A component that manages the distribution of elements across a cache cluster
 *
 * @author Manik Surtani
 * @since 4.0
 */
@Scope(Scopes.GLOBAL)
public interface DistributionManager {

   void rehash();

   boolean isLocal(Object key);

   /**
    * Locates a key in a cluster.  The returned addresses <i>may not</i> be owners of the keys if a rehash happens to
    * be in progress or is pending, so when querying these servers, invalid responses should be checked for and the
    * next address checked accordingly.
    * @param key key to test
    * @return a list of addresses where the key may reside
    */
   List<Address> locate(Object key);

   /**
    * Locates a list of keys in a cluster.  Like {@link #locate(Object)} the returned addresses <i>may not</i> be owners
    * of the keys if a rehash happens to be in progress or is pending, so when querying these servers, invalid responses
    * should be checked for and the next address checked accordingly.
    * @param keys list of keys to test
    * @return a list of addresses where the key may reside
    */
   Map<Object, List<Address>> locate(List<Object> keys);
}

