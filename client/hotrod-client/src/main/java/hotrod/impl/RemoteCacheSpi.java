package hotrod.impl;

import hotrod.ClusterTopologyListener;
import hotrod.RemoteCacheManager;
import hotrod.impl.VersionedEntry;

/**
 * // TODO: Document this
 *
 * - TODO - add timeout support
 * - TODO - add flags support
 * - TODO - enforce encoding and add such tests
 *
 * @author mmarkus
 * @since 4.1
 */
public interface RemoteCacheSpi {

   public enum VersionedOperationResponse {
      SUCCESS(true), NO_SUCH_KEY(false), MODIFIED_KEY(false);
      private boolean isModified;

      VersionedOperationResponse(boolean modified) {
         isModified = modified;
      }

      public boolean isUpdated() {
         return isModified;
      }
   }

   public byte[] get(byte[] key);

   public boolean remove(byte[] key);

   public boolean contains(byte[] key);

   public VersionedEntry getVersionedCacheEntry(byte[] key);

   /**
    * @return true if this there is an entry for that key in the cache(which is overwritten now), false otherwise.
    */
   public void put(byte[] key, byte[] value);

   public boolean putIfAbsent(byte[] key, byte[] value);

   public boolean replace(byte[] key, byte[] value);

   public VersionedOperationResponse replaceIfUnmodified(byte[] key, byte[] value, long version);

   public VersionedOperationResponse removeIfUnmodified(byte[] key, long version);

   public void putForExternalRead(byte[] key, byte[] value);

   /**
    * @param key the key to be evicted.
    * @return true if the key was evicted, false if it does not exist or could not be evicted.
    */
   public boolean evict(byte[] key);

   public void clear();

   public String stats();

   public String stats(String paramName);

   public void addClusterTopologyListener(ClusterTopologyListener listener);

   public boolean removeClusterTopologyListener(ClusterTopologyListener listener);

   public RemoteCacheManager getRemoteCacheFactory();
}
