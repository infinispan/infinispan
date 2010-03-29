package org.infinispan.client.hotrod.impl;

import java.util.Arrays;

/**
* // TODO: Document this
*
* @author mmarkus
* @since 4.1
*/
public class BinaryVersionedValue {
   private final long version;
   private final byte[] value;

   public BinaryVersionedValue(long version, byte[] value) {
      this.version = version;
      this.value = value;
   }

   public long getVersion() {
      return version;
   }

   public byte[] getValue() {
      return value;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      BinaryVersionedValue that = (BinaryVersionedValue) o;

      return version == that.version && Arrays.equals(value, that.value);
   }

   @Override
   public int hashCode() {
      int result = (int) (version ^ (version >>> 32));
      result = 31 * result + (value != null ? Arrays.hashCode(value) : 0);
      return result;
   }
}
