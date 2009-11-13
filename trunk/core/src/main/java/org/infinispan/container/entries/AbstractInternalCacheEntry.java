package org.infinispan.container.entries;

import org.infinispan.container.DataContainer;

/**
 * An abstract internal cache entry that is typically stored in the data container
 *
 * @author Manik Surtani
 * @since 4.0
 */
public abstract class AbstractInternalCacheEntry implements InternalCacheEntry {

   Object key;

   AbstractInternalCacheEntry() {
   }

   AbstractInternalCacheEntry(Object key) {
      this.key = key;
   }

   public final void commit(DataContainer container) {
      // no-op
   }

   public final void rollback() {
      // no-op
   }

   public final void setCreated(boolean created) {
      // no-op
   }

   public final void setRemoved(boolean removed) {
      // no-op
   }

   public final void setValid(boolean valid) {
      // no-op
   }

   public final boolean isNull() {
      return false;
   }

   public final boolean isChanged() {
      return false;
   }

   public final boolean isCreated() {
      return false;
   }

   public final boolean isRemoved() {
      return false;
   }

   public final boolean isValid() {
      return false;
   }

   public final Object getKey() {
      return key;
   }

   @Override
   public String toString() {
      return getClass().getSimpleName() + "{" +
            "key=" + key +
            '}';
   }

   public AbstractInternalCacheEntry clone() {
      try {
         return (AbstractInternalCacheEntry) super.clone();
      } catch (CloneNotSupportedException e) {
         throw new RuntimeException("Should never happen!", e);
      }
   }
}
