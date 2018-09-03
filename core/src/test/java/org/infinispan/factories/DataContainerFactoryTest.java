package org.infinispan.factories;


import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import org.infinispan.commons.util.Features;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.container.impl.BoundedSegmentedDataContainer;
import org.infinispan.container.impl.DefaultDataContainer;
import org.infinispan.container.impl.DefaultSegmentedDataContainer;
import org.infinispan.container.impl.L1SegmentedDataContainer;
import org.infinispan.container.offheap.BoundedOffHeapDataContainer;
import org.infinispan.container.offheap.OffHeapConcurrentMap;
import org.infinispan.container.offheap.OffHeapDataContainer;
import org.infinispan.container.offheap.SegmentedBoundedOffHeapDataContainer;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(testName = "factories.DataContainerFactoryTest", groups = "functional")
public class DataContainerFactoryTest extends AbstractInfinispanTest {
   private static final String COMPONENT_NAME = "";

   private DataContainerFactory dataContainerFactory;

   @BeforeMethod
   public void before() {
      dataContainerFactory = new DataContainerFactory();
      dataContainerFactory.globalConfiguration = GlobalConfigurationBuilder.defaultClusteredBuilder().build();
   }

   @Test
   public void testDefaultConfigurationDataContainer() {
      dataContainerFactory.configuration = new ConfigurationBuilder().build();
      assertEquals(DefaultDataContainer.class, dataContainerFactory.construct(COMPONENT_NAME).getClass());
   }

   @Test
   public void testOffHeap() {
      dataContainerFactory.configuration = new ConfigurationBuilder()
            .memory().storageType(StorageType.OFF_HEAP).build();
      assertEquals(OffHeapDataContainer.class, dataContainerFactory.construct(COMPONENT_NAME).getClass());
   }

   @Test
   public void testSegmentedOffHeap() {
      dataContainerFactory.configuration = new ConfigurationBuilder().clustering().cacheMode(CacheMode.DIST_ASYNC)
            .memory().storageType(StorageType.OFF_HEAP).build();

      assertTrue(dataContainerFactory.globalConfiguration.features().isAvailable(DataContainerFactory.SEGMENTATION_FEATURE));
      assertEquals(DefaultSegmentedDataContainer.class, dataContainerFactory.construct(COMPONENT_NAME).getClass());
   }

   @Test
   public void testSegmentedOffHeapAndL1() {
      dataContainerFactory = spy(new DataContainerFactory());
      doReturn(mock(OffHeapConcurrentMap.class))
            .when(dataContainerFactory)
            .createAndStartOffHeapConcurrentMap(anyInt(), anyInt());

      dataContainerFactory.globalConfiguration = GlobalConfigurationBuilder.defaultClusteredBuilder().build();
      dataContainerFactory.configuration = new ConfigurationBuilder().clustering()
            .cacheMode(CacheMode.DIST_ASYNC)
            .l1().enable()
            .memory().storageType(StorageType.OFF_HEAP).build();

      assertEquals(L1SegmentedDataContainer.class, this.dataContainerFactory.construct(COMPONENT_NAME).getClass());
   }

   @Test
   public void testDefaultSegmented() {
      dataContainerFactory.configuration = new ConfigurationBuilder().clustering()
            .cacheMode(CacheMode.DIST_ASYNC).build();

      assertEquals(DefaultSegmentedDataContainer.class, this.dataContainerFactory.construct(COMPONENT_NAME).getClass());
   }

   @Test
   public void testSegmentedL1() {
      dataContainerFactory.configuration = new ConfigurationBuilder().clustering()
            .cacheMode(CacheMode.DIST_ASYNC)
            .l1().enable().build();

      assertEquals(L1SegmentedDataContainer.class, this.dataContainerFactory.construct(COMPONENT_NAME).getClass());
   }

   @Test
   public void testEvictionRemoveNotSegmented() {
      dataContainerFactory.configuration = new ConfigurationBuilder().clustering()
            .memory().evictionStrategy(EvictionStrategy.REMOVE).size(1000).build();

      assertEquals(DefaultDataContainer.class, this.dataContainerFactory.construct(COMPONENT_NAME).getClass());
   }

   @Test
   public void testEvictionRemoveSegmented() {
      dataContainerFactory.configuration = new ConfigurationBuilder().clustering()
            .memory().evictionStrategy(EvictionStrategy.REMOVE).size(1000)
            .clustering().cacheMode(CacheMode.DIST_ASYNC).build();

      assertEquals(BoundedSegmentedDataContainer.class, this.dataContainerFactory.construct(COMPONENT_NAME).getClass());
   }

   @Test
   public void testEvictionRemoveNotSegmentedOffHeap() {
      dataContainerFactory.configuration = new ConfigurationBuilder().clustering()
            .memory().storageType(StorageType.OFF_HEAP).evictionStrategy(EvictionStrategy.REMOVE).size(1000)
            .build();

      assertEquals(BoundedOffHeapDataContainer.class, this.dataContainerFactory.construct(COMPONENT_NAME).getClass());
   }

   @Test
   public void testEvictionRemoveSegmentedOffHeap() {
      dataContainerFactory.configuration = new ConfigurationBuilder().clustering()
            .memory().storageType(StorageType.OFF_HEAP).evictionStrategy(EvictionStrategy.REMOVE).size(1000)
            .clustering().cacheMode(CacheMode.DIST_ASYNC).build();

      assertEquals(SegmentedBoundedOffHeapDataContainer.class, this.dataContainerFactory.construct(
         COMPONENT_NAME).getClass());
   }

   @Test
   public void testNotAvailableFeature() {
      Features features = mock(Features.class);
      dataContainerFactory.globalConfiguration = mock(GlobalConfiguration.class);
      when(dataContainerFactory.globalConfiguration.features()).thenReturn(features);
      when(features.isAvailable(DataContainerFactory.SEGMENTATION_FEATURE)).thenReturn(false);

      dataContainerFactory.configuration = new ConfigurationBuilder().clustering()
            .memory().storageType(StorageType.OFF_HEAP).evictionStrategy(EvictionStrategy.REMOVE).size(1000)
            .clustering().cacheMode(CacheMode.DIST_ASYNC).build();

      assertEquals(BoundedOffHeapDataContainer.class, this.dataContainerFactory.construct(COMPONENT_NAME).getClass());
   }
}
