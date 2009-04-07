package org.infinispan.container.entries;

import org.infinispan.container.DataContainer;

import java.io.Serializable;

/**
 * An abstract internal cache entry that is typically stored in the data container
 *
 * @author Manik Surtani
 * @since 4.0
 */
public abstract class AbstractInternalCacheEntry implements InternalCacheEntry, Serializable {

   Object key;
   Object value;

   AbstractInternalCacheEntry() {
   }

   AbstractInternalCacheEntry(Object key, Object value) {
      this.key = key;
      this.value = value;
   }

   public final Object setValue(Object value) {
      Object old = this.value;
      this.value = value;
      return old;
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

   public final Object getValue() {
      return value;
   }
}
