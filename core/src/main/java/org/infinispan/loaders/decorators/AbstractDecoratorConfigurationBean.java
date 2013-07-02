package org.infinispan.loaders.decorators;

import org.infinispan.config.AbstractNamedCacheConfigurationBean;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.loaders.AbstractCacheStoreConfig;
import org.infinispan.loaders.CacheStoreConfig;

import javax.xml.bind.annotation.XmlTransient;

/**
 * Class to aid decorators to be able to fluently modify parent properties.
 * I.e. async store to enable navigating to cache store configuration.
 *
 * @author Galder Zamarreño
 * @since 5.0
 */
public abstract class AbstractDecoratorConfigurationBean
      extends AbstractNamedCacheConfigurationBean implements CacheStoreConfig {

   private AbstractCacheStoreConfig cacheStoreConfig;

   public void setCacheStoreConfig(AbstractCacheStoreConfig cacheStoreConfig) {
      this.cacheStoreConfig = cacheStoreConfig;
   }

   @Override
   @XmlTransient
   public String getCacheLoaderClassName() {
      return cacheStoreConfig.getCacheLoaderClassName();
   }

   @Override
   public void setCacheLoaderClassName(String s) {
      cacheStoreConfig.setCacheLoaderClassName(s);
   }

   @Override
   @XmlTransient
   public Boolean isPurgeOnStartup() {
      return cacheStoreConfig.isPurgeOnStartup();
   }

   @Override
   @XmlTransient
   public Boolean isFetchPersistentState() {
      return cacheStoreConfig.isFetchPersistentState();
   }

   @Override
   public void setFetchPersistentState(Boolean fetchPersistentState) {
      cacheStoreConfig.setFetchPersistentState(fetchPersistentState);
   }

   @Override
   public void setIgnoreModifications(Boolean ignoreModifications) {
      cacheStoreConfig.setIgnoreModifications(ignoreModifications);
   }

   @Override
   public CacheStoreConfig fetchPersistentState(Boolean fetchPersistentState) {
      return cacheStoreConfig.fetchPersistentState(fetchPersistentState);
   }

   @Override
   public CacheStoreConfig ignoreModifications(Boolean ignoreModifications) {
      return cacheStoreConfig.ignoreModifications(ignoreModifications);
   }

   @Override
   @XmlTransient
   public Boolean isIgnoreModifications() {
      return cacheStoreConfig.isIgnoreModifications();
   }

   @Override
   public void setPurgeOnStartup(Boolean purgeOnStartup) {
      cacheStoreConfig.setPurgeOnStartup(purgeOnStartup);
   }

   @Override
   public CacheStoreConfig purgeOnStartup(Boolean purgeOnStartup) {
      return cacheStoreConfig.purgeOnStartup(purgeOnStartup);
   }

   @Override
   @XmlTransient
   public SingletonStoreConfig getSingletonStoreConfig() {
      return cacheStoreConfig.getSingletonStoreConfig();
   }

   @Override
   public void setSingletonStoreConfig(SingletonStoreConfig singletonStoreConfig) {
      cacheStoreConfig.setSingletonStoreConfig(singletonStoreConfig);
   }

   @Override
   @XmlTransient
   public AsyncStoreConfig getAsyncStoreConfig() {
      return cacheStoreConfig.getAsyncStoreConfig();
   }

   @Override
   public void setAsyncStoreConfig(AsyncStoreConfig asyncStoreConfig) {
      cacheStoreConfig.setAsyncStoreConfig(asyncStoreConfig);
   }

   @Override
   @XmlTransient
   public Boolean isPurgeSynchronously() {
      return cacheStoreConfig.isPurgeSynchronously();
   }

   @Override
   public void setPurgeSynchronously(Boolean purgeSynchronously) {
      cacheStoreConfig.setPurgeSynchronously(purgeSynchronously);
   }

   @Override
   public CacheStoreConfig purgeSynchronously(Boolean purgeSynchronously) {
      return cacheStoreConfig.purgeSynchronously(purgeSynchronously);
   }

   @Override
   public CacheStoreConfig purgerThreads(Integer purgerThreads) {
      return cacheStoreConfig.purgerThreads(purgerThreads);
   }

   @Override
   public Integer getPurgerThreads() {
      return cacheStoreConfig.getPurgerThreads();
   }

   @Override
   public AsyncStoreConfig asyncStore() {
      return cacheStoreConfig.asyncStore();
   }

   @Override
   public SingletonStoreConfig singletonStore() {
      return cacheStoreConfig.singletonStore();
   }

   /**
    * Get back up to the <code>CacheStoreConfig</code> level.
    */
   public AbstractCacheStoreConfig build() {
      return cacheStoreConfig;
   }

   @Override
   public AbstractDecoratorConfigurationBean clone() {
      try {
         AbstractDecoratorConfigurationBean dolly = (AbstractDecoratorConfigurationBean) super.clone();
         if (cr != null)
            dolly.cr = (ComponentRegistry) cr.clone();
         return dolly;
      } catch (CloneNotSupportedException cnse) {
         throw new RuntimeException("Should not happen!", cnse);
      }
   }

   @Override
   public ClassLoader getClassLoader() {
      return cacheStoreConfig.getClassLoader();
   }
   
}
