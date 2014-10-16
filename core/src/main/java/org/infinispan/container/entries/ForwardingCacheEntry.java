package org.infinispan.container.entries;

import org.infinispan.container.DataContainer;
import org.infinispan.metadata.Metadata;

/**
 * A class designed to forward all method invocations for a CacheEntry to the provided delegate.  This
 * class is useful to extend when you want to only modify
 *
 * @author wburns
 * @since 7.0
 */
public abstract class ForwardingCacheEntry<K, V> implements CacheEntry<K, V> {
   protected abstract CacheEntry<K, V> delegate();

   @Override
   public boolean isNull() {
      return delegate().isNull();
   }

   @Override
   public boolean isChanged() {
      return delegate().isChanged();
   }

   @Override
   public boolean isCreated() {
      return delegate().isCreated();
   }

   @Override
   public boolean isRemoved() {
      return delegate().isRemoved();
   }

   @Override
   public boolean isEvicted() {
      return delegate().isEvicted();
   }

   @Override
   public boolean isValid() {
      return delegate().isValid();
   }

   @Override
   public boolean isLoaded() {
      return delegate().isLoaded();
   }

   @Override
   public K getKey() {
      return delegate().getKey();
   }

   @Override
   public V getValue() {
      return delegate().getValue();
   }

   @Override
   public long getLifespan() {
      return delegate().getLifespan();
   }

   @Override
   public long getMaxIdle() {
      return delegate().getMaxIdle();
   }

   @Override
   public boolean skipLookup() {
      return delegate().skipLookup();
   }

   @Override
   public V setValue(V value) {
      return delegate().setValue(value);
   }

   @Override
   public void commit(DataContainer container, Metadata metadata) {
      delegate().commit(container, metadata);
   }

   @Override
   public void rollback() {
      delegate().rollback();
   }

   @Override
   public void setChanged(boolean changed) {
      delegate().setChanged(changed);
   }

   @Override
   public void setCreated(boolean created) {
      delegate().setCreated(created);
   }

   @Override
   public void setRemoved(boolean removed) {
      delegate().setRemoved(removed);
   }

   @Override
   public void setEvicted(boolean evicted) {
      delegate().setEvicted(evicted);
   }

   @Override
   public void setValid(boolean valid) {
      delegate().setValid(valid);
   }

   @Override
   public void setLoaded(boolean loaded) {
      delegate().setLoaded(loaded);
   }

   @Override
   public void setSkipLookup(boolean skipLookup) {
      delegate().setSkipLookup(skipLookup);
   }

   @Override
   public boolean undelete(boolean doUndelete) {
      return delegate().undelete(doUndelete);
   }

   @Override
   public CacheEntry<K, V> clone() {
      return delegate().clone();
   }

   @Override
   public Metadata getMetadata() {
      return delegate().getMetadata();
   }

   @Override
   public void setMetadata(Metadata metadata) {
      delegate().setMetadata(metadata);
   }

   @Override
   public String toString() {
      return delegate().toString();
   }

   // We already break equals contract in several places when comparing all the various CacheEntry
   // types as the same ones
   @Override
   public boolean equals(Object obj) {
      return delegate().equals(obj);
   }

   // We already break hashcode contract in several places when comparing all the various CacheEntry
   // types as the same ones
   @Override
   public int hashCode() {
      return delegate().hashCode();
   }
}
