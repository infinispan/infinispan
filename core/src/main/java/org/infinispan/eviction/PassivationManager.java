package org.infinispan.eviction;

import net.jcip.annotations.ThreadSafe;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.persistence.CacheLoaderException;

/**
 * A passivation manager
 *
 * @author Manik Surtani
 * @version 4.1
 */
@ThreadSafe
@Scope(Scopes.NAMED_CACHE)
public interface PassivationManager {
   
   boolean isEnabled();

   void passivate(InternalCacheEntry entry);

   void passivateAll() throws CacheLoaderException;

   long getPassivationCount();

   void resetPassivationCount();   
}
