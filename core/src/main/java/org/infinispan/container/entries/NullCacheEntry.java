package org.infinispan.container.entries;

import org.infinispan.container.DataContainer;
import org.infinispan.metadata.Metadata;

public class NullCacheEntry<K, V> implements CacheEntry<K, V> {

   private static final NullCacheEntry INSTANCE = new NullCacheEntry();

   private NullCacheEntry() {
   }

   public static <K, V> NullCacheEntry<K, V> getInstance() {
      return INSTANCE;
   }

   @Override
   public boolean isNull() {
      return true;
   }

   @Override
   public boolean isChanged() {
      return false;
   }

   @Override
   public boolean isCreated() {
      return false;
   }

   @Override
   public boolean isRemoved() {
      return false;
   }

   @Override
   public boolean isEvicted() {
      return false;
   }

   @Override
   public K getKey() {
      return null;
   }

   @Override
   public V getValue() {
      return null;
   }

   @Override
   public long getLifespan() {
      return -1;
   }

   @Override
   public long getMaxIdle() {
      return -1;
   }

   @Override
   public boolean skipLookup() {
      return false;
   }

   @Override
   public Object setValue(Object value) {
      return null;
   }

   @Override
   public void commit(DataContainer container) {
      // No-op
   }

   @Override
   public void setChanged(boolean changed) {
      // No-op
   }

   @Override
   public void setCreated(boolean created) {
      // No-op
   }

   @Override
   public void setRemoved(boolean removed) {
      // No-op
   }

   @Override
   public void setEvicted(boolean evicted) {
      // No-op
   }

   @Override
   public void setSkipLookup(boolean skipLookup) {
      // No-op
   }

   @Override
   public CacheEntry clone() {
      return INSTANCE;
   }

   @Override
   public Metadata getMetadata() {
      return null;
   }

   @Override
   public void setMetadata(Metadata metadata) {
      // No-op
   }

   @Override
   public String toString() {
      return "NullCacheEntry{}";
   }
}
