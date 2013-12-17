package org.infinispan.container.entries;

import org.infinispan.metadata.Metadata;
import org.infinispan.container.DataContainer;

/**
 * An abstract internal cache entry that is typically stored in the data container
 *
 * @author Manik Surtani
 * @since 4.0
 */
public abstract class AbstractInternalCacheEntry implements InternalCacheEntry {

   protected Object key;

   protected AbstractInternalCacheEntry() {
   }

   protected AbstractInternalCacheEntry(Object key) {
      this.key = key;
   }

   @Override
   public final void commit(DataContainer container, Metadata metadata) {
      // no-op
   }

   @Override
   public final void rollback() {
      // no-op
   }

   @Override
   public void setChanged(boolean changed) {
      // no-op
   }

   @Override
   public final void setCreated(boolean created) {
      // no-op
   }

   @Override
   public final void setRemoved(boolean removed) {
      // no-op
   }

   @Override
   public final void setEvicted(boolean evicted) {
      // no-op
   }

   @Override
   public final void setValid(boolean valid) {
      // no-op
   }

   @Override
   public void setLoaded(boolean loaded) {
      // no-op
   }

   @Override
   public void setSkipLookup(boolean skipLookup) {
      //no-op
   }

   @Override
   public final boolean isNull() {
      return false;
   }

   @Override
   public final boolean isChanged() {
      return false;
   }

   @Override
   public final boolean isCreated() {
      return false;
   }

   @Override
   public final boolean isRemoved() {
      return false;
   }

   @Override
   public final boolean isEvicted() {
      return true;
   }

   @Override
   public final boolean isValid() {
      return false;
   }

   @Override
   public boolean isLoaded() {
      return false;
   }

   @Override
   public boolean skipLookup() {
      return false;
   }

   @Override
   public boolean undelete(boolean doUndelete) {
      return false;
   }

   @Override
   public Metadata getMetadata() {
      return null;
   }

   @Override
   public void setMetadata(Metadata metadata) {
      // no-op
   }

   @Override
   public final Object getKey() {
      return key;
   }

   @Override
   public String toString() {
      return getClass().getSimpleName() + "{" +
            "key=" + key +
            '}';
   }

   @Override
   public AbstractInternalCacheEntry clone() {
      try {
         return (AbstractInternalCacheEntry) super.clone();
      } catch (CloneNotSupportedException e) {
         throw new RuntimeException("Should never happen!", e);
      }
   }
}
