package org.infinispan.context.impl;

import java.util.concurrent.CompletionStage;

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
      return null;
   }

   @Override
   public final void setClassLoader(final ClassLoader classLoader) {
      // No-op
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

   @Override
   public CompletionStage<Void> enter() {
      return null;  // TODO: Customise this generated block
   }

   @Override
   public void exit() {
      // TODO: Customise this generated block
   }

   @Override
   public InvocationContext clone() {
      try {
         return (InvocationContext) super.clone();
      } catch (CloneNotSupportedException e) {
         throw new IllegalStateException("Impossible!", e);
      }
   }
}
