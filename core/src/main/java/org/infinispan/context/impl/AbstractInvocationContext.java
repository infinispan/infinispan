package org.infinispan.context.impl;

import java.util.Collection;

import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.remoting.transport.Address;
import org.reactivestreams.Publisher;

import io.reactivex.rxjava3.core.Flowable;

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
   public boolean isEntryRemovedInContext(final Object key) {
      CacheEntry ce = lookupEntry(key);
      return ce != null && ce.isRemoved() && ce.isChanged();
   }

   @Override
   public InvocationContext clone() {
      try {
         return (InvocationContext) super.clone();
      } catch (CloneNotSupportedException e) {
         throw new IllegalStateException("Impossible!", e);
      }
   }

   @Override
   public <K, V> Publisher<CacheEntry<K, V>> publisher() {
      if (lookedUpEntriesCount() == 0) {
         return Flowable.empty();
      }
      Collection<CacheEntry<K, V>> collection = (Collection) getLookedUpEntries().values();
      return Flowable.fromIterable(collection)
            .filter(ce -> !ce.isRemoved() && !ce.isNull());
   }
}
