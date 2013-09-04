package org.infinispan.configuration.cache;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.ConfigurationFor;
import org.infinispan.commons.util.TypedProperties;
import org.infinispan.configuration.cache.FileCacheStoreConfigurationBuilder.FsyncMode;
import org.infinispan.loaders.file.DelegateFileCacheStore;
import org.infinispan.loaders.file.FileCacheStore;

/**
 * File cache store configuration.
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
@BuiltBy(FileCacheStoreConfigurationBuilder.class)
@ConfigurationFor(DelegateFileCacheStore.class)
public class FileCacheStoreConfiguration extends AbstractLockSupportStoreConfiguration {

   private final String location;
   private final long fsyncInterval;
   private final FsyncMode fsyncMode;
   private final int streamBufferSize;
   private final int maxEntries;
   private final boolean deprecatedBucketFormat;

   FileCacheStoreConfiguration(String location, int maxEntries, boolean deprecatedBucketFormat,
         long fsyncInterval, FsyncMode fsyncMode, int streamBufferSize, long lockAcquistionTimeout,
         int lockConcurrencyLevel, boolean purgeOnStartup, boolean purgeSynchronously,
         int purgerThreads, boolean fetchPersistentState, boolean ignoreModifications,
         TypedProperties properties, AsyncStoreConfiguration async,
         SingletonStoreConfiguration singletonStore) {
      super(lockAcquistionTimeout, lockConcurrencyLevel, purgeOnStartup,
            purgeSynchronously, purgerThreads, fetchPersistentState,
            ignoreModifications, properties, async, singletonStore);
      this.location = location;
      this.maxEntries = maxEntries;
      this.deprecatedBucketFormat = deprecatedBucketFormat;
      this.fsyncInterval = fsyncInterval;
      this.fsyncMode = fsyncMode;
      this.streamBufferSize = streamBufferSize;
   }

   @Deprecated
   public long fsyncInterval() {
      return fsyncInterval;
   }

   @Deprecated
   public FsyncMode fsyncMode() {
      return fsyncMode;
   }

   public String location() {
      return location;
   }

   public boolean deprecatedBucketFormat() {
      return deprecatedBucketFormat;
   }

   public int maxEntries() {
      return maxEntries;
   }

   @Deprecated
   public int streamBufferSize() {
      return streamBufferSize;
   }

   @Override
   public String toString() {
      return "FileCacheStoreConfiguration{" +
            "fsyncInterval=" + fsyncInterval +
            ", location='" + location + '\'' +
            ", maxEntries=" + maxEntries +
            ", deprecatedBucketFormat=" + deprecatedBucketFormat +
            ", fsyncMode=" + fsyncMode +
            ", streamBufferSize=" + streamBufferSize +
            ", lockAcquistionTimeout=" + lockAcquistionTimeout() +
            ", lockConcurrencyLevel=" + lockConcurrencyLevel() +
            ", purgeOnStartup=" + purgeOnStartup() +
            ", purgeSynchronously=" + purgeSynchronously() +
            ", purgerThreads=" + purgerThreads() +
            ", fetchPersistentState=" + fetchPersistentState() +
            ", ignoreModifications=" + ignoreModifications() +
            ", properties=" + properties() +
            ", async=" + async() +
            ", singletonStore=" + singletonStore() +
            '}';
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      if (!super.equals(o)) return false;

      FileCacheStoreConfiguration that = (FileCacheStoreConfiguration) o;

      if (fsyncInterval != that.fsyncInterval) return false;
      if (streamBufferSize != that.streamBufferSize) return false;
      if (fsyncMode != that.fsyncMode) return false;
      if (location != null ? !location.equals(that.location) : that.location != null)
         return false;
      if (maxEntries != that.maxEntries) return false;
      if (deprecatedBucketFormat != that.deprecatedBucketFormat) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = super.hashCode();
      result = 31 * result + (location != null ? location.hashCode() : 0);
      result = 31 * result + maxEntries;
      result = 31 * result + (deprecatedBucketFormat ? 1 : 0);
      result = 31 * result + (int) (fsyncInterval ^ (fsyncInterval >>> 32));
      result = 31 * result + (fsyncMode != null ? fsyncMode.hashCode() : 0);
      result = 31 * result + streamBufferSize;
      return result;
   }

}
