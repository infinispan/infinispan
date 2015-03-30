package org.jboss.as.clustering.infinispan.cs.factory;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.ConfiguredBy;
import org.infinispan.configuration.cache.CustomStoreConfiguration;
import org.infinispan.configuration.cache.CustomStoreConfigurationBuilder;
import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.configuration.cache.StoreConfigurationBuilder;

/**
 * Metadata for deployed Cache Store
 *
 * @author Sebastian Laskawiec
 * @since 7.2
 */
public class DeployedCacheStoreMetadata {

   private String loaderWriterInstanceName;
   private Object loaderWriterRawInstance;
   private Class<? extends StoreConfiguration> storeConfigurationClass;
   private Class<? extends StoreConfigurationBuilder> storeBuilderClass;

   public DeployedCacheStoreMetadata(Object loaderWriterRawInstance) {
      this.loaderWriterRawInstance = loaderWriterRawInstance;
      this.loaderWriterInstanceName = loaderWriterRawInstance.getClass().getName();
   }

   public static DeployedCacheStoreMetadata fromDeployedStoreInstance(Object loaderWriterRawInstance) {
      DeployedCacheStoreMetadata ret = new DeployedCacheStoreMetadata(loaderWriterRawInstance);
      ret.initializeConfigurationData();
      return ret;
   }

   private void initializeConfigurationData() {
      ConfiguredBy configuredBy = loaderWriterRawInstance.getClass().getAnnotation(ConfiguredBy.class);
      if (configuredBy != null) {
         if (configuredBy.value() == null) {
            throw new IllegalArgumentException("Cache Store's configuration class is incorrect");
         }
         this.storeConfigurationClass = (Class<? extends StoreConfiguration>) configuredBy.value();
         BuiltBy builtBy = storeConfigurationClass.getAnnotation(BuiltBy.class);
         if (builtBy != null) {
            if (builtBy.value() == null) {
               throw new IllegalArgumentException("Cache Store's configuration builder class is incorrect");
            }
            this.storeBuilderClass = builtBy.value().asSubclass(StoreConfigurationBuilder.class);
         } else {
            this.storeBuilderClass = CustomStoreConfigurationBuilder.class;
         }
      } else {
         this.storeConfigurationClass = CustomStoreConfiguration.class;
         this.storeBuilderClass = CustomStoreConfigurationBuilder.class;
      }
   }

   public String getDeployedCacheClassName() {
      return loaderWriterInstanceName;
   }

   public Object getLoaderWriterRawInstance() {
      return loaderWriterRawInstance;
   }

   public Class<? extends StoreConfiguration> getStoreConfigurationClass() {
      return storeConfigurationClass;
   }

   public Class<? extends StoreConfigurationBuilder> getStoreBuilderClass() {
      return storeBuilderClass;
   }

   @Override
   public String toString() {
      return "DeployedCacheStoreMetadata{" +
            "loaderWriterInstanceName='" + loaderWriterInstanceName + '\'' +
            ", loaderWriterRawInstance=" + loaderWriterRawInstance +
            ", storeConfigurationClass=" + storeConfigurationClass +
            ", storeBuilderClass=" + storeBuilderClass +
            '}';
   }
}
