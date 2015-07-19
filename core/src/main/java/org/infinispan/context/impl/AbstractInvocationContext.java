package org.infinispan.context.impl;

import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.remoting.transport.Address;

/**
 * Common features of transaction and invocation contexts
 *
 * @author Manik Surtani
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public abstract class AbstractInvocationContext implements InvocationContext {
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
   public AbstractInvocationContext clone() {
      try {
         return (AbstractInvocationContext) super.clone();
      } catch (CloneNotSupportedException e) {
         throw new IllegalStateException("Impossible!");
      }
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
   public boolean replaceValue(final Object key, final InternalCacheEntry cacheEntry) {
      CacheEntry ce = lookupEntry(key);
      if (ce == null || ce.isNull() || ce.getValue() == null) {
         if (ce != null) {
            ce.setValue(cacheEntry.getValue());
            ce.setMetadata(cacheEntry.getMetadata());
            onEntryValueReplaced(key, cacheEntry);
         } else {
            return false;
         }
      }
      return true;
   }

   @Override
   public boolean isEntryRemovedInContext(final Object key) {
      CacheEntry ce = lookupEntry(key);
      return ce != null && ce.isRemoved() && ce.isChanged();
   }

   protected void onEntryValueReplaced(final Object key, final InternalCacheEntry cacheEntry) {
      //no-op. used in tx mode with write skew check.
   }
}
