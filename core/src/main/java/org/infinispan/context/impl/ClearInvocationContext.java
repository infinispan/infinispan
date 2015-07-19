package org.infinispan.context.impl;

import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.ClearCacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.remoting.transport.Address;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * Implementation of {@link org.infinispan.context.InvocationContext} used by the {@link
 * org.infinispan.commands.write.ClearCommand}.
 *
 * @author Pedro Ruivo
 * @since 7.2
 */
public class ClearInvocationContext extends AbstractInvocationContext implements Cloneable {

   private static final Map<Object, CacheEntry> LOOKUP_ENTRIES = Collections.singletonMap((Object) "_clear_", (CacheEntry) ClearCacheEntry.getInstance());

   public ClearInvocationContext(Address origin) {
      super(origin);
   }

   @Override
   public boolean isInTxScope() {
      return false;
   }

   @Override
   public Object getLockOwner() {
      return Thread.currentThread();
   }

   @Override
   public ClearInvocationContext clone() {
      return (ClearInvocationContext) super.clone();
   }

   @Override
   public Set<Object> getLockedKeys() {
      return Collections.emptySet();
   }

   @Override
   public void clearLockedKeys() {
      /*no-op*/
   }

   @Override
   public void addLockedKey(Object key) {
      /*no-op*/
   }

   @Override
   public boolean hasLockedKey(Object key) {
      //ClearCommand does not acquire locks
      return false;
   }

   @Override
   public boolean replaceValue(Object key, InternalCacheEntry cacheEntry) {
      return true;
   }

   @Override
   public boolean isEntryRemovedInContext(Object key) {
      //clear remove all entries
      return true;
   }

   @Override
   public CacheEntry lookupEntry(Object key) {
      return null;
   }

   @Override
   public Map<Object, CacheEntry> getLookedUpEntries() {
      return LOOKUP_ENTRIES;
   }

   @Override
   public void putLookedUpEntry(Object key, CacheEntry e) {
      /*no-op*/
   }

   @Override
   public void removeLookedUpEntry(Object key) {
      /*no-op*/
   }
}
