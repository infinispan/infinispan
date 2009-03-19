package org.horizon.container;

public class CachedValue implements MVCCEntry {
   protected Object value;
   protected long modifiedTime;

   protected CachedValue() {
   }

   public CachedValue(Object value) {
      this.value = value;
      touch();
   }

   public final void touch() {
      modifiedTime = System.currentTimeMillis();
   }

   public final long getModifiedTime() {
      return modifiedTime;
   }

   public Object getKey() {
      return null;
   }

   public final Object getValue() {
      return value;
   }

   public final Object setValue(Object value) {
      this.value = value;
      return null;
   }

   public final boolean isNullEntry() {
      return false;
   }

   public void commitUpdate(DataContainer container) {
   }

   public void rollbackUpdate() {
   }

   public final boolean isChanged() {
      return false;
   }

   public final boolean isCreated() {
      return false;
   }

   public void setCreated(boolean created) {
      throw new UnsupportedOperationException();
   }

   public final boolean isDeleted() {
      return false;
   }

   public void setDeleted(boolean deleted) {
      throw new UnsupportedOperationException();
   }

   public final boolean isValid() {
      return false;
   }

   public void setValid(boolean valid) {
      throw new UnsupportedOperationException();
   }

   public long getLifespan() {
      return -1;
   }

   public void setLifespan(long lifespan) {
      throw new UnsupportedOperationException();
   }
}