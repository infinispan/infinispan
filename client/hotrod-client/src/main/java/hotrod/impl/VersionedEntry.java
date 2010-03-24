package hotrod.impl;

import java.util.Arrays;

/**
* // TODO: Document this
*
* @author mmarkus
* @since 4.1
*/
public class VersionedEntry {
   private final long version;
   private final byte[] key;
   private final byte[] value;

   public VersionedEntry(long version, byte[] key, byte[] value) {
      this.version = version;
      this.key = key;
      this.value = value;
   }

   public long getVersion() {
      return version;
   }

   public byte[] getKey() {
      return key;
   }

   public byte[] getValue() {
      return value;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      VersionedEntry that = (VersionedEntry) o;

      if (version != that.version) return false;
      if (!Arrays.equals(key, that.key)) return false;
      if (!Arrays.equals(value, that.value)) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = (int) (version ^ (version >>> 32));
      result = 31 * result + (key != null ? Arrays.hashCode(key) : 0);
      result = 31 * result + (value != null ? Arrays.hashCode(value) : 0);
      return result;
   }
}
