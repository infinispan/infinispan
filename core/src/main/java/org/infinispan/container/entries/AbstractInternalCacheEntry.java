package org.infinispan.container.entries;

import static org.infinispan.commons.util.Util.toStr;

import java.util.Map;
import java.util.Objects;

import org.infinispan.container.DataContainer;
import org.infinispan.metadata.Metadata;

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
   public final void commit(DataContainer container) {
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
   public void setSkipLookup(boolean skipLookup) {
      //no-op
   }

   @Override
   public final boolean isNull() {
      return getValue() == null;
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
   public boolean skipLookup() {
      return true;
   }

   @Override
   public Metadata getMetadata() {
      return null;
   }

   @Override
   public void setMetadata(Metadata metadata) {
      // no-op
   }

   protected boolean hasMetadata() {
      return false;
   }

   @Override
   public final Object getKey() {
      return key;
   }

   @Override
   public boolean isL1Entry() {
      return false;
   }

   @Override
   public String toString() {
      String className = getClass().getSimpleName();
      StringBuilder sb = new StringBuilder(className)
            .append("{key=").append(toStr(key))
            .append(", value=").append(toStr(getValue()));
      if (hasMetadata()) {
            sb.append(", metadata=").append(getMetadata());
      }
      return sb.append('}').toString();
   }

   @Override
   public AbstractInternalCacheEntry clone() {
      try {
         return (AbstractInternalCacheEntry) super.clone();
      } catch (CloneNotSupportedException e) {
         throw new RuntimeException("Should never happen!", e);
      }
   }

   @Override
   public final boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || !(o instanceof Map.Entry)) return false;

      Map.Entry that = (Map.Entry) o;

      return Objects.equals(getKey(), that.getKey()) && Objects.equals(getValue(), that.getValue());
   }

   @Override
   public final int hashCode() {
      return  31 * Objects.hashCode(getKey()) + Objects.hashCode(getValue());
   }
}
