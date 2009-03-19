package org.horizon.loader;

import org.horizon.config.PluggableConfigurationComponent;
import org.horizon.loader.decorators.AsyncStoreConfig;
import org.horizon.loader.decorators.SingletonStoreConfig;
import org.horizon.util.Util;

public class AbstractCacheLoaderConfig extends PluggableConfigurationComponent implements CacheLoaderConfig {
   private boolean ignoreModifications;
   private boolean fetchPersistentState;
   private boolean purgeOnStartup;
   private SingletonStoreConfig singletonStoreConfig = new SingletonStoreConfig();
   private AsyncStoreConfig asyncStoreConfig = new AsyncStoreConfig();

   public boolean isPurgeOnStartup() {
      return purgeOnStartup;
   }

   public boolean isFetchPersistentState() {
      return fetchPersistentState;
   }

   public void setFetchPersistentState(boolean fetchPersistentState) {
      testImmutability("fetchPersistentState");
      this.fetchPersistentState = fetchPersistentState;
   }

   public void setIgnoreModifications(boolean ignoreModifications) {
      testImmutability("ignoreModifications");
      this.ignoreModifications = ignoreModifications;
   }

   public boolean isIgnoreModifications() {
      return ignoreModifications;
   }

   public void setPurgeOnStartup(boolean purgeOnStartup) {
      testImmutability("purgeOnStartup");
      this.purgeOnStartup = purgeOnStartup;
   }

   public SingletonStoreConfig getSingletonStoreConfig() {
      return singletonStoreConfig;
   }

   public void setSingletonStoreConfig(SingletonStoreConfig singletonStoreConfig) {
      testImmutability("singletonStoreConfig");
      this.singletonStoreConfig = singletonStoreConfig;
   }

   public AsyncStoreConfig getAsyncStoreConfig() {
      return asyncStoreConfig;
   }

   public void setAsyncStoreConfig(AsyncStoreConfig asyncStoreConfig) {
      testImmutability("asyncStoreConfig");
      this.asyncStoreConfig = asyncStoreConfig;
   }

   @Override
   public boolean equals(Object obj) {
      if (super.equals(obj)) {
         if (!(obj instanceof AbstractCacheLoaderConfig)) return false;
         AbstractCacheLoaderConfig i = (AbstractCacheLoaderConfig) obj;
         return equalsExcludingProperties(i);
      }
      return false;
   }

   protected boolean equalsExcludingProperties(Object obj) {
      AbstractCacheLoaderConfig other = (AbstractCacheLoaderConfig) obj;

      return Util.safeEquals(this.className, other.className)
            && (this.ignoreModifications == other.ignoreModifications)
            && (this.fetchPersistentState == other.fetchPersistentState)
            && Util.safeEquals(this.singletonStoreConfig, other.singletonStoreConfig)
            && Util.safeEquals(this.asyncStoreConfig, other.asyncStoreConfig);
   }

   @Override
   public int hashCode() {
      return 31 * hashCodeExcludingProperties() + (properties == null ? 0 : properties.hashCode());
   }

   protected int hashCodeExcludingProperties() {
      int result = 17;
      result = 31 * result + (className == null ? 0 : className.hashCode());
      result = 31 * result + (ignoreModifications ? 0 : 1);
      result = 31 * result + (fetchPersistentState ? 0 : 1);
      result = 31 * result + (singletonStoreConfig == null ? 0 : singletonStoreConfig.hashCode());
      result = 31 * result + (asyncStoreConfig == null ? 0 : asyncStoreConfig.hashCode());
      result = 31 * result + (purgeOnStartup ? 0 : 1);
      return result;
   }

   @Override
   public String toString() {
      return new StringBuilder().append(getClass().getSimpleName()).append("{").append("className='").append(className).append('\'')
            .append(", ignoreModifications=").append(ignoreModifications)
            .append(", fetchPersistentState=").append(fetchPersistentState)
            .append(", properties=").append(properties)
            .append(", purgeOnStartup=").append(purgeOnStartup).append("},")
            .append(", SingletonStoreConfig{").append(singletonStoreConfig).append('}')
            .append(", AsyncStoreConfig{").append(asyncStoreConfig).append('}')
            .toString();
   }

   @Override
   public AbstractCacheLoaderConfig clone() {
      AbstractCacheLoaderConfig clone = null;
      try {
         clone = (AbstractCacheLoaderConfig) super.clone();
      } catch (CloneNotSupportedException e) {
         throw new RuntimeException("Should not happen!", e);
      }
      if (singletonStoreConfig != null) clone.setSingletonStoreConfig(singletonStoreConfig.clone());
      if (asyncStoreConfig != null) clone.setAsyncStoreConfig(asyncStoreConfig.clone());
      return clone;
   }
}
