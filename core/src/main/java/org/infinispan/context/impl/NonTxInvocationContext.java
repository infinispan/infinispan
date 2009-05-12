package org.infinispan.context.impl;

import org.infinispan.container.entries.CacheEntry;
import org.infinispan.util.BidirectionalMap;
import org.infinispan.util.InfinispanCollections;
import org.infinispan.util.BidirectionalLinkedHashMap;

import java.util.Map;

/**
 * // TODO: Mircea: Document this!
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public class NonTxInvocationContext extends AbstractInvocationContext {

   private boolean isOriginLocal;

   protected BidirectionalLinkedHashMap<Object, CacheEntry> lookedUpEntries = null;

   public CacheEntry lookupEntry(Object k) {
      return lookedUpEntries == null ? null : lookedUpEntries.get(k);
   }

   public void removeLookedUpEntry(Object key) {
      if (lookedUpEntries != null) lookedUpEntries.remove(key);
   }

   public void putLookedUpEntry(Object key, CacheEntry e) {
      initLookedUpEntries();
      lookedUpEntries.put(key, e);
   }

   public void putLookedUpEntries(Map<Object, CacheEntry> lookedUpEntries) {
      initLookedUpEntries();
      this.lookedUpEntries.putAll(lookedUpEntries);
   }

   public void clearLookedUpEntries() {
      if (lookedUpEntries != null) lookedUpEntries.clear();
   }

   @SuppressWarnings("unchecked")
   public BidirectionalMap<Object, CacheEntry> getLookedUpEntries() {
      return (BidirectionalMap<Object, CacheEntry>)
            (lookedUpEntries == null ? InfinispanCollections.emptyBidirectionalMap() : lookedUpEntries);
   }

   public boolean isOriginLocal() {
      return isOriginLocal;
   }

   public void setOriginLocal(boolean originLocal) {
      isOriginLocal = originLocal;
   }

   public boolean isInTxScope() {
      return false;
   }

   public Object getLockOwner() {
      return Thread.currentThread();
   }

   private void initLookedUpEntries() {
      if (lookedUpEntries == null) lookedUpEntries = new BidirectionalLinkedHashMap<Object, CacheEntry>(4);
   }

   public void prepareForCall() {
      resetFlags();
      clearLookedUpEntries();
   }

   @Override
   public Object clone() {
      NonTxInvocationContext dolly = (NonTxInvocationContext) super.clone();
      if (lookedUpEntries != null) {
         dolly.lookedUpEntries = new BidirectionalLinkedHashMap<Object, CacheEntry>(lookedUpEntries);
      }
      return dolly; 
   }
}
