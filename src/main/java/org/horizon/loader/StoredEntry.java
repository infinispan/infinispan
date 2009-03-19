package org.horizon.loader;

import org.horizon.container.ExpirableCachedValue;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * An entry that is stored in a cache loader.
 *
 * @author Manik Surtani
 */
public class StoredEntry extends ExpirableCachedValue implements Externalizable {
   private Object key;

   public StoredEntry() {
   }

   public StoredEntry(Object key, Object value) {
      super(value, System.currentTimeMillis(), -1);
      this.key = key;
   }

   public StoredEntry(Object key, Object value, long created, long expiry) {
      super(value, created, expiry);
      this.key = key;
   }

   public StoredEntry(Object key, Object value, long lifespan) {
      super(value, System.currentTimeMillis());
      this.key = key;
      setLifespan(lifespan);
   }

   @Override
   public final Object getKey() {
      return key;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      StoredEntry that = (StoredEntry) o;

      if (key != null ? !key.equals(that.key) : that.key != null) return false;

      return true;
   }

   @Override
   public int hashCode() {
      return key != null ? key.hashCode() : 0;
   }

   @Override
   public String toString() {
      return "StoredEntry{" +
            "key=" + key +
            ", value=" + value +
            ", createdTime=" + createdTime +
            ", expiryTime=" + expiryTime +
            '}';
   }

   public void writeExternal(ObjectOutput out) throws IOException {
      out.writeObject(key);
      out.writeObject(value);
      out.writeLong(createdTime);
      out.writeLong(expiryTime);
   }

   @SuppressWarnings("unchecked")
   public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
      key = in.readObject();
      value = in.readObject();
      createdTime = in.readLong();
      expiryTime = in.readLong();
      modifiedTime = System.currentTimeMillis();
   }
}
