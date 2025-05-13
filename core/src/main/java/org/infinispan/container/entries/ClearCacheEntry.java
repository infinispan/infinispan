package org.infinispan.container.entries;

import org.infinispan.container.DataContainer;
import org.infinispan.metadata.Metadata;

/**
 * Used in {@link org.infinispan.context.impl.ClearInvocationContext} to process the {@link
 * org.infinispan.commands.write.ClearCommand}.
 *
 * @author Pedro Ruivo
 * @since 7.2
 */
public class ClearCacheEntry<K, V> implements CacheEntry<K, V> {

   //singleton, we have no state
   private static final ClearCacheEntry INSTANCE = new ClearCacheEntry();

   private ClearCacheEntry() {
   }

   public static <K, V> ClearCacheEntry<K, V> getInstance() {
      //noinspection unchecked
      return INSTANCE;
   }

   @Override
   public boolean isNull() {
      return true;
   }

   @Override
   public boolean isChanged() {
      return true;
   }

   @Override
   public void setChanged(boolean changed) {
      /*no-op*/
   }

   @Override
   public boolean isCreated() {
      return false;
   }

   @Override
   public void setCreated(boolean created) {
      /*no-op*/
   }

   @Override
   public boolean isRemoved() {
      return true;
   }

   @Override
   public void setRemoved(boolean removed) {
      /*no-op*/
   }

   @Override
   public boolean isEvicted() {
      return false;
   }

   @Override
   public void setEvicted(boolean evicted) {
      /*no-op*/
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
      return true;
   }

   @Override
   public V setValue(V value) {
      /*-no-op*/
      return null;
   }

   @Override
   public void commit(DataContainer<K, V> container) {
      container.clear();
   }

      @Override
   public void setSkipLookup(boolean skipLookup) {
      /*no-op*/
   }

   @Override
   public final CacheEntry<K, V> clone() {
      return getInstance(); //no clone. singleton
   }

   @Override
   public Metadata getMetadata() {
      return null;
   }

   @Override
   public void setMetadata(Metadata metadata) {
      /*no-op*/
   }

   @Override
   public String toString() {
      return "ClearCacheEntry{}";
   }
}
