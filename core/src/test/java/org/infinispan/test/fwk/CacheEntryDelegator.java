package org.infinispan.test.fwk;

import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.metadata.Metadata;

/**
 * A {@link org.infinispan.container.entries.CacheEntry} delegator
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public class CacheEntryDelegator implements CacheEntry {

   private final CacheEntry delegate;

   public CacheEntryDelegator(CacheEntry delegate) {
      this.delegate = delegate;
   }

   @Override
   public boolean isNull() {
      return delegate.isNull();
   }

   @Override
   public Metadata getMetadata() {
      return delegate.getMetadata();
   }

   @Override
   public void setMetadata(Metadata metadata) {
      delegate.setMetadata(metadata);
   }

   @Override
   public boolean isChanged() {
      return delegate.isChanged();
   }

   @Override
   public boolean isCreated() {
      return delegate.isCreated();
   }

   @Override
   public boolean isRemoved() {
      return delegate.isRemoved();
   }

   @Override
   public boolean isEvicted() {
      return delegate.isEvicted();
   }

   @Override
   public boolean isValid() {
      return delegate.isValid();
   }

   @Override
   public boolean isLoaded() {
      return delegate.isLoaded();
   }

   @Override
   public Object getKey() {
      return delegate.getKey();
   }

   @Override
   public Object getValue() {
      return delegate.getValue();
   }

   @Override
   public long getLifespan() {
      return delegate.getLifespan();
   }

   @Override
   public long getMaxIdle() {
      return delegate.getMaxIdle();
   }

   @Override
   public boolean skipLookup() {
      return delegate.skipLookup();
   }

   @Override
   public Object setValue(Object value) {
      return delegate.setValue(value);
   }

   @Override
   public void commit(DataContainer container, Metadata metadata) {
      delegate.commit(container, metadata);
   }

   @Override
   public void rollback() {
      delegate.rollback();
   }

   @Override
   public void setChanged(boolean changed) {
      delegate.setChanged(changed);
   }

   @Override
   public void setCreated(boolean created) {
      delegate.setCreated(created);
   }

   @Override
   public void setRemoved(boolean removed) {
      delegate.setRemoved(removed);
   }

   @Override
   public void setEvicted(boolean evicted) {
      delegate.setEvicted(evicted);
   }

   @Override
   public void setValid(boolean valid) {
      delegate.setValid(valid);
   }

   @Override
   public void setLoaded(boolean loaded) {
      delegate.setLoaded(loaded);
   }

   @Override
   public void setSkipLookup(boolean skipLookup) {
      delegate.setSkipLookup(skipLookup);
   }

   @Override
   public boolean undelete(boolean doUndelete) {
      return delegate.undelete(doUndelete);
   }

   @Override
   public CacheEntry clone() {
      try {
         return (CacheEntry) super.clone();
      } catch (CloneNotSupportedException e) {
         throw new AssertionError(e);
      }
   }

}
