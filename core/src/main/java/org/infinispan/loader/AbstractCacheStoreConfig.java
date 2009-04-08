package org.infinispan.loader;

import org.infinispan.config.PluggableConfigurationComponent;
import org.infinispan.loader.decorators.AsyncStoreConfig;
import org.infinispan.loader.decorators.SingletonStoreConfig;
import org.infinispan.util.Util;

/**
 * Configures {@link AbstractCacheStore}.  This allows you to tune a number of characteristics of the {@link
 * AbstractCacheStore}.
 * <p/>
 * <ul> <li><tt>purgeSynchronously</tt> - whether {@link org.infinispan.loader.CacheStore#purgeExpired()} calls happen
 * synchronously or not.  By default, this is set to <tt>false</tt>.</li>
 * <p/>
 * </ul>
 *
 * @author Mircea.Markus@jboss.com
 * @version $Id$
 * @since 4.0
 */
public class AbstractCacheStoreConfig extends PluggableConfigurationComponent implements CacheStoreConfig {
   private boolean ignoreModifications;
   private boolean fetchPersistentState;
   private boolean purgeOnStartup;
   private SingletonStoreConfig singletonStoreConfig = new SingletonStoreConfig();
   private AsyncStoreConfig asyncStoreConfig = new AsyncStoreConfig();

   private boolean purgeSynchronously = false;
   protected String cacheLoaderClassName;

   public boolean isPurgeSynchronously() {
      return purgeSynchronously;
   }

   public void setPurgeSynchronously(boolean purgeSynchronously) {
      testImmutability("purgeSynchronously");
      this.purgeSynchronously = purgeSynchronously;
   }

   public String getCacheLoaderClassName() {
      return cacheLoaderClassName;
   }

   public void setCacheLoaderClassName(String className) {
      if (className == null || className.length() == 0) return;
      testImmutability("cacheLoaderClassName");
      this.cacheLoaderClassName = className;
   }

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
         if (!(obj instanceof AbstractCacheStoreConfig)) return false;
         AbstractCacheStoreConfig i = (AbstractCacheStoreConfig) obj;
         return equalsExcludingProperties(i);
      }
      return false;
   }

   protected boolean equalsExcludingProperties(Object obj) {
      AbstractCacheStoreConfig other = (AbstractCacheStoreConfig) obj;

      return Util.safeEquals(this.cacheLoaderClassName, other.cacheLoaderClassName)
            && (this.ignoreModifications == other.ignoreModifications)
            && (this.fetchPersistentState == other.fetchPersistentState)
            && Util.safeEquals(this.singletonStoreConfig, other.singletonStoreConfig)
            && Util.safeEquals(this.asyncStoreConfig, other.asyncStoreConfig)
            && Util.safeEquals(this.purgeSynchronously, other.purgeSynchronously);
   }

   @Override
   public int hashCode() {
      return 31 * hashCodeExcludingProperties() + (properties == null ? 0 : properties.hashCode());
   }

   protected int hashCodeExcludingProperties() {
      int result = 17;
      result = 31 * result + (cacheLoaderClassName == null ? 0 : cacheLoaderClassName.hashCode());
      result = 31 * result + (ignoreModifications ? 0 : 1);
      result = 31 * result + (fetchPersistentState ? 0 : 1);
      result = 31 * result + (singletonStoreConfig == null ? 0 : singletonStoreConfig.hashCode());
      result = 31 * result + (asyncStoreConfig == null ? 0 : asyncStoreConfig.hashCode());
      result = 31 * result + (purgeOnStartup ? 0 : 1);
      return result;
   }

   @Override
   public String toString() {
      return new StringBuilder().append(getClass().getSimpleName()).append("{").append("className='").append(cacheLoaderClassName).append('\'')
            .append(", ignoreModifications=").append(ignoreModifications)
            .append(", fetchPersistentState=").append(fetchPersistentState)
            .append(", properties=").append(properties)
            .append(", purgeOnStartup=").append(purgeOnStartup).append("},")
            .append(", SingletonStoreConfig{").append(singletonStoreConfig).append('}')
            .append(", AsyncStoreConfig{").append(asyncStoreConfig).append('}')
            .append(", purgeSynchronously{").append(purgeSynchronously).append('}')
            .toString();
   }

   @Override
   public AbstractCacheStoreConfig clone() {
      AbstractCacheStoreConfig clone = null;
      try {
         clone = (AbstractCacheStoreConfig) super.clone();
      } catch (CloneNotSupportedException e) {
         throw new RuntimeException("Should not happen!", e);
      }
      if (singletonStoreConfig != null) clone.setSingletonStoreConfig(singletonStoreConfig.clone());
      if (asyncStoreConfig != null) clone.setAsyncStoreConfig(asyncStoreConfig.clone());
      return clone;
   }
}
