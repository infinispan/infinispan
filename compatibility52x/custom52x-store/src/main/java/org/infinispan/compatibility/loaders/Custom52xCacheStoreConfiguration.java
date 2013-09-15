package org.infinispan.compatibility.loaders;

import org.infinispan.config.parsing.XmlConfigHelper;
import org.infinispan.configuration.BuiltBy;
import org.infinispan.configuration.cache.AbstractLockSupportStoreConfiguration;
import org.infinispan.configuration.cache.AsyncStoreConfiguration;
import org.infinispan.configuration.cache.LegacyConfigurationAdaptor;
import org.infinispan.configuration.cache.LegacyLoaderAdapter;
import org.infinispan.configuration.cache.SingletonStoreConfiguration;
import org.infinispan.util.TypedProperties;

/**
 * @author Mircea Markus
 * @since 6.0
 */
@BuiltBy(Custom52xCacheStoreConfigurationBuilder.class)
public class Custom52xCacheStoreConfiguration extends AbstractLockSupportStoreConfiguration implements LegacyLoaderAdapter<Custom52xCacheStoreConfig> {

   private final String location;
   private final long fsyncInterval;
   private final Custom52xCacheStoreConfigurationBuilder.FsyncMode fsyncMode;
   private final int streamBufferSize;

   Custom52xCacheStoreConfiguration(String location, long fsyncInterval,
                                    Custom52xCacheStoreConfigurationBuilder.FsyncMode fsyncMode, int streamBufferSize, long lockAcquistionTimeout,
                                    int lockConcurrencyLevel, boolean purgeOnStartup, boolean purgeSynchronously,
                                    int purgerThreads, boolean fetchPersistentState, boolean ignoreModifications,
                                    TypedProperties properties, AsyncStoreConfiguration async,
                                    SingletonStoreConfiguration singletonStore) {
      super(lockAcquistionTimeout, lockConcurrencyLevel, purgeOnStartup,
            purgeSynchronously, purgerThreads, fetchPersistentState,
            ignoreModifications, properties, async, singletonStore);
      this.location = location;
      this.fsyncInterval = fsyncInterval;
      this.fsyncMode = fsyncMode;
      this.streamBufferSize = streamBufferSize;
   }

   public long fsyncInterval() {
      return fsyncInterval;
   }

   public Custom52xCacheStoreConfigurationBuilder.FsyncMode fsyncMode() {
      return fsyncMode;
   }

   public String location() {
      return location;
   }

   public int streamBufferSize() {
      return streamBufferSize;
   }

   @Override
   public String toString() {
      return "Custom52xCacheStoreConfiguration{" +
            "fsyncInterval=" + fsyncInterval +
            ", location='" + location + '\'' +
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

      Custom52xCacheStoreConfiguration that = (Custom52xCacheStoreConfiguration) o;

      if (fsyncInterval != that.fsyncInterval) return false;
      if (streamBufferSize != that.streamBufferSize) return false;
      if (fsyncMode != that.fsyncMode) return false;
      if (location != null ? !location.equals(that.location) : that.location != null)
         return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = super.hashCode();
      result = 31 * result + (location != null ? location.hashCode() : 0);
      result = 31 * result + (int) (fsyncInterval ^ (fsyncInterval >>> 32));
      result = 31 * result + (fsyncMode != null ? fsyncMode.hashCode() : 0);
      result = 31 * result + streamBufferSize;
      return result;
   }

   @Override
   public Custom52xCacheStoreConfig adapt() {
      Custom52xCacheStoreConfig config = new Custom52xCacheStoreConfig();

      LegacyConfigurationAdaptor.adapt(this, config);

      config.fsyncInterval(fsyncInterval);
      config.fsyncMode(Custom52xCacheStoreConfig.FsyncMode.valueOf(fsyncMode.name()));
      config.streamBufferSize(streamBufferSize);
      config.location(location);

      XmlConfigHelper.setValues(config, properties(), false, true);

      return config;
   }

}
