package org.infinispan.container.entries;

import java.util.Map;
import java.util.Objects;

import org.infinispan.commons.util.Util;
import org.infinispan.container.DataContainer;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.metadata.Metadata;
import org.infinispan.metadata.impl.PrivateMetadata;
import org.infinispan.protostream.annotations.ProtoField;

/**
 * An abstract internal cache entry that is typically stored in the data container
 *
 * @author Manik Surtani
 * @since 4.0
 */
public abstract class AbstractInternalCacheEntry implements InternalCacheEntry {

   protected Object key;
   protected Object value;
   protected PrivateMetadata internalMetadata;

   protected AbstractInternalCacheEntry(Object key, Object value, PrivateMetadata internalMetadata) {
      this.key = key;
      this.value = value;
      this.internalMetadata = internalMetadata;
   }

   protected AbstractInternalCacheEntry(MarshallableObject<?> wrappedKey, MarshallableObject<?> wrappedValue,
                                        PrivateMetadata internalMetadata) {
      this(MarshallableObject.unwrap(wrappedKey), MarshallableObject.unwrap(wrappedValue), internalMetadata);
   }

   @ProtoField(number = 1, name = "key")
   public MarshallableObject<?> getWrappedKey() {
      return MarshallableObject.create(key);
   }

   @ProtoField(number = 2,  name = "value")
   public MarshallableObject<?> getWrappedValue() {
      return MarshallableObject.create(value);
   }

   @Override
   @ProtoField(3)
   public PrivateMetadata getInternalMetadata() {
      return internalMetadata;
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

   @Override
   public final Object getKey() {
      return key;
   }

   @Override
   public final Object getValue() {
      return value;
   }

   @Override
   public final Object setValue(Object value) {
      Object old = this.value;
      this.value = value;
      return old;
   }

   @Override
   public boolean isL1Entry() {
      return false;
   }

   @Override
   public final void setInternalMetadata(PrivateMetadata metadata) {
      this.internalMetadata = metadata;
   }

   @Override
   public final String toString() {
      StringBuilder sb = new StringBuilder(getClass().getSimpleName());
      sb.append('{');
      appendFieldsToString(sb);
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
      if (!(o instanceof Map.Entry)) return false;

      Map.Entry that = (Map.Entry) o;

      return Objects.equals(getKey(), that.getKey()) && Objects.equals(getValue(), that.getValue());
   }

   @Override
   public final int hashCode() {
      return  31 * Objects.hashCode(getKey()) + Objects.hashCode(getValue());
   }

   protected void appendFieldsToString(StringBuilder builder) {
      builder.append("key=").append(Util.toStr(key));
      builder.append(", value=").append(Util.toStr(value));
      builder.append(", internalMetadata=").append(internalMetadata);
   }
}
