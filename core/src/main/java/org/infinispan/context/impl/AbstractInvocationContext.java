package org.infinispan.context.impl;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Common features of transaction and invocation contexts
 *
 * @author Manik Surtani
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public abstract class AbstractInvocationContext implements InvocationContext {
   private static Log log = LogFactory.getLog(AbstractInvocationContext.class);
   private static boolean trace = log.isTraceEnabled();

   private static final AtomicReferenceFieldUpdater<AbstractInvocationContext, Object> contextLockUpdater =
         AtomicReferenceFieldUpdater.newUpdater(AbstractInvocationContext.class, Object.class, "contextLock");

   private final Address origin;
   private volatile Object contextLock = ContextLock.init();

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
      if (trace) log.trace("Entering context");
      return ContextLock.enter(this, contextLockUpdater);
   }

   @Override
   public void exit() {
      if (trace) log.trace("Leaving context");
      ContextLock.exit(this, contextLockUpdater);
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
