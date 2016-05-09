package org.infinispan.context.impl;

import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.impl.BaseAsyncInvocationContext;
import org.infinispan.remoting.transport.Address;

/**
 * Common features of transaction and invocation contexts
 *
 * @author Manik Surtani
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public abstract class AbstractInvocationContext extends BaseAsyncInvocationContext implements InvocationContext {
   private final Address origin;
   // Class loader associated with this invocation which supports AdvancedCache.with() functionality
   private ClassLoader classLoader;

   protected AbstractInvocationContext(Address origin) {
      this.origin = origin;
   }

   @Override
   public final Address getOrigin() {
      return origin;
   }

   @Override
   public boolean isOriginLocal() {
      return origin == null;
   }

   @Override
   public boolean hasLockedKey(Object key) {
      return getLockedKeys().contains(key);
   }

   @Override
   public final ClassLoader getClassLoader() {
      return classLoader;
   }

   @Override
   public final void setClassLoader(final ClassLoader classLoader) {
      this.classLoader = classLoader;
   }

   @Override
   public boolean isEntryRemovedInContext(final Object key) {
      CacheEntry ce = lookupEntry(key);
      return ce != null && ce.isRemoved() && ce.isChanged();
   }

   /**
    * @deprecated Since 8.1, no longer used.
    */
   @Deprecated
   protected void onEntryValueReplaced(final Object key, final InternalCacheEntry cacheEntry) {
   }
}
