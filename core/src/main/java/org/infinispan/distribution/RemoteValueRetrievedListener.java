package org.infinispan.distribution;

import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;

/**
 * Listener that is notified when a remote value is looked up
 *
 * @author William Burns
 * @since 6.0
 */
@Scope(Scopes.NAMED_CACHE)
public interface RemoteValueRetrievedListener {
   /**
    * Invoked when a remote value is found from a remote source
    * @param ice The cache entry that was found
    */
   public void remoteValueFound(InternalCacheEntry ice);

   /**
    * Invoked when a remote value is not found from the remote source for the given key
    * @param key The key for which there was no value found
    */
   public void remoteValueNotFound(Object key);
}
