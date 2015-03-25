package org.jboss.as.clustering.infinispan.cs.factory;

import org.infinispan.configuration.cache.CustomStoreConfiguration;
import org.infinispan.configuration.cache.CustomStoreConfigurationBuilder;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class DeployedCacheStoreMetadataTest {

   @Test
   public void testImportingMetadataWithLoaderWriterClassOnly() throws Exception {
      //given
      Object loaderWriterRawInstance = new CustomStoreWithoutConfiguration();

      //when
      DeployedCacheStoreMetadata deployedCacheStoreMetadata = DeployedCacheStoreMetadata.fromDeployedStoreInstance(loaderWriterRawInstance);

      //then
      assertEquals(CustomStoreConfiguration.class, deployedCacheStoreMetadata.getStoreConfigurationClass());
      assertEquals(CustomStoreConfigurationBuilder.class, deployedCacheStoreMetadata.getStoreBuilderClass());
      assertEquals(loaderWriterRawInstance, deployedCacheStoreMetadata.getLoaderWriterRawInstance());
      assertEquals(loaderWriterRawInstance.getClass().getName(), deployedCacheStoreMetadata.getDeployedCacheClassName());
   }

   @Test
   public void testImportingMetadataWithLoaderWriterWithConfiguration() throws Exception {
      //given
      Object loaderWriterRawInstance = new CustomStoreWithConfiguration();

      //when
      DeployedCacheStoreMetadata deployedCacheStoreMetadata = DeployedCacheStoreMetadata.fromDeployedStoreInstance(loaderWriterRawInstance);

      //then
      assertEquals(CustomStoreConfigurationWithoutBuilder.class, deployedCacheStoreMetadata.getStoreConfigurationClass());
      assertEquals(CustomStoreConfigurationBuilder.class, deployedCacheStoreMetadata.getStoreBuilderClass());
   }

   @Test
   public void testImportingMetadataWithLoaderWriterWithConfigurationAndBuilder() throws Exception {
      //given
      Object loaderWriterRawInstance = new CustomStoreWithConfigurationAndBuilder();

      //when
      DeployedCacheStoreMetadata deployedCacheStoreMetadata = DeployedCacheStoreMetadata.fromDeployedStoreInstance(loaderWriterRawInstance);

      //then
      assertEquals(CustomStoreConfigurationWithBuilder.class, deployedCacheStoreMetadata.getStoreConfigurationClass());
      assertEquals(org.jboss.as.clustering.infinispan.cs.factory.CustomStoreConfigurationBuilder.class, deployedCacheStoreMetadata.getStoreBuilderClass());
   }
}