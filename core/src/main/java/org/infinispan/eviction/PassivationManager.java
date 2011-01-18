package org.infinispan.eviction;

import net.jcip.annotations.ThreadSafe;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.loaders.CacheLoaderException;

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

   void passivate(Object key, InternalCacheEntry entry, InvocationContext ctx) throws CacheLoaderException;

   void passivateAll() throws CacheLoaderException;

   long getPassivationCount();

   void resetPassivationCount();   
}
