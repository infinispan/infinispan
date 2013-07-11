package org.infinispan.loaders.file;

import org.infinispan.loaders.AbstractCacheStoreConfig;

/**
 * Configures {@link SingleFileCacheStore}.
 * <p/>
 * <ul>
 * <li><tt>location</tt> - a location on disk where the store can write internal files. This
 * defaults to <tt>Infinispan-SingleFileCacheStore</tt> in the current working directory.</li>
 * <li><tt>maxEntries</tt> - maximum number of entries allowed in the cache store.
 * If more entries are added, the least recently used (LRU) entry is removed.
 * Since this cache store's space requirements are split between memory and
 * filesystem, setting a limit to the size of the cache store primarily helps avoid
 * OutOfMemoryExceptions due to memory consumption part of this cache store.
 * However, this limit results in removing entries from the cache store and hence
 * should only be set in situations where the cache store contents can be
 * regenerated, in other words, when Infinispan is used as a cache, as opposed to
 * an authoritative data store. </li>
 * </ul>
 * 
 * @author Karsten Blees
 * @since 6.0
 */
public class SingleFileCacheStoreConfig extends AbstractCacheStoreConfig {
   private static final long serialVersionUID = 1L;

   private String location = "Infinispan-SingleFileCacheStore";

   private int maxEntries = -1;

   public SingleFileCacheStoreConfig() {
      setCacheLoaderClassName(SingleFileCacheStore.class.getName());
   }

   public String getLocation() {
      return location;
   }

   public void setLocation(String location) {
      testImmutability("location");
      this.location = location;
   }

   public SingleFileCacheStoreConfig location(String location) {
      setLocation(location);
      return this;
   }

   public int getMaxEntries() {
      return maxEntries;
   }

   public void setMaxEntries(int maxEntries) {
      testImmutability("maxEntries");
      this.maxEntries = maxEntries;
   }

   public SingleFileCacheStoreConfig maxEntries(int maxEntries) {
      setMaxEntries(maxEntries);
      return this;
   }

}
