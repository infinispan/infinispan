package org.jboss.as.clustering.infinispan.cs.factory;

import static org.junit.Assert.assertEquals;

import java.util.function.Supplier;

import org.infinispan.configuration.cache.CustomStoreConfigurationBuilder;
import org.junit.Test;

public class DeployedCacheStoreMetadataTest {

   @Test
   public void testImportingMetadataWithLoaderWriterClassOnly() {
      //given
      Supplier<Object> instanceSupplier = CustomStoreWithoutConfiguration::new;
      Object loaderWriterRawInstance = instanceSupplier.get();

      //when
      DeployedCacheStoreMetadata deployedCacheStoreMetadata = DeployedCacheStoreMetadata.fromDeployedStoreInstance(instanceSupplier);

      //then
      assertEquals(CustomStoreConfigurationBuilder.class, deployedCacheStoreMetadata.getStoreBuilderClass());
      assertEquals(loaderWriterRawInstance.getClass().getName(), deployedCacheStoreMetadata.getDeployedCacheClassName());
   }

   @Test
   public void testImportingMetadataWithLoaderWriterWithConfiguration() {
      //given
      Supplier<Object> instanceSupplier = CustomStoreWithConfiguration::new;

      //when
      DeployedCacheStoreMetadata deployedCacheStoreMetadata = DeployedCacheStoreMetadata.fromDeployedStoreInstance(instanceSupplier);

      //then
      assertEquals(CustomStoreConfigurationBuilder.class, deployedCacheStoreMetadata.getStoreBuilderClass());
   }

   @Test
   public void testImportingMetadataWithLoaderWriterWithConfigurationAndBuilder() {
      //when
      DeployedCacheStoreMetadata deployedCacheStoreMetadata = DeployedCacheStoreMetadata.fromDeployedStoreInstance(CustomStoreWithConfigurationAndBuilder::new);

      //then
      assertEquals(org.jboss.as.clustering.infinispan.cs.factory.CustomStoreConfigurationBuilder.class, deployedCacheStoreMetadata.getStoreBuilderClass());
   }
}
