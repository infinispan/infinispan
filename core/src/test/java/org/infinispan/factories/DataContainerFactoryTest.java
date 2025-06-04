package org.infinispan.factories;


import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.testng.AssertJUnit.assertEquals;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
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
            .memory().storage(StorageType.OFF_HEAP).build();
      assertEquals(OffHeapDataContainer.class, dataContainerFactory.construct(COMPONENT_NAME).getClass());
   }

   @Test
   public void testSegmentedOffHeap() {
      dataContainerFactory.configuration = new ConfigurationBuilder().clustering().cacheMode(CacheMode.DIST_ASYNC)
            .memory().storage(StorageType.OFF_HEAP).build();

      Object component = dataContainerFactory.construct(COMPONENT_NAME);
      assertEquals(DefaultSegmentedDataContainer.class, component.getClass());
   }

   @Test
   public void testSegmentedOffHeapAndL1() {
      dataContainerFactory = spy(new DataContainerFactory());
      doReturn(mock(OffHeapConcurrentMap.class))
            .when(dataContainerFactory)
            .createAndStartOffHeapConcurrentMap();

      dataContainerFactory.globalConfiguration = GlobalConfigurationBuilder.defaultClusteredBuilder().build();
      dataContainerFactory.configuration = new ConfigurationBuilder().clustering()
            .cacheMode(CacheMode.DIST_ASYNC)
            .l1().enable()
            .memory().storage(StorageType.OFF_HEAP).build();

      Object component = dataContainerFactory.construct(COMPONENT_NAME);
      assertEquals(L1SegmentedDataContainer.class, component.getClass());
   }

   @Test
   public void testDefaultSegmented() {
      dataContainerFactory.configuration = new ConfigurationBuilder().clustering()
            .cacheMode(CacheMode.DIST_ASYNC).build();

      Object component = dataContainerFactory.construct(COMPONENT_NAME);
      assertEquals(DefaultSegmentedDataContainer.class, component.getClass());
   }

   @Test
   public void testSegmentedL1() {
      dataContainerFactory.configuration = new ConfigurationBuilder().clustering()
            .cacheMode(CacheMode.DIST_ASYNC)
            .l1().enable().build();

      Object component = dataContainerFactory.construct(COMPONENT_NAME);
      assertEquals(L1SegmentedDataContainer.class, component.getClass());
   }

   @Test
   public void testEvictionRemoveNotSegmented() {
      dataContainerFactory.configuration = new ConfigurationBuilder().clustering()
            .memory().whenFull(EvictionStrategy.REMOVE).maxCount(1000).build();

      assertEquals(DefaultDataContainer.class, this.dataContainerFactory.construct(COMPONENT_NAME).getClass());
   }

   @Test
   public void testEvictionRemoveSegmented() {
      dataContainerFactory.configuration = new ConfigurationBuilder().clustering()
            .memory().whenFull(EvictionStrategy.REMOVE).maxCount(1000)
            .clustering().cacheMode(CacheMode.DIST_ASYNC).build();

      Object component = dataContainerFactory.construct(COMPONENT_NAME);
      assertEquals(BoundedSegmentedDataContainer.class, component.getClass());
   }

   @Test
   public void testEvictionRemoveNotSegmentedOffHeap() {
      dataContainerFactory.configuration = new ConfigurationBuilder().clustering()
            .memory().storage(StorageType.OFF_HEAP).whenFull(EvictionStrategy.REMOVE).maxCount(1000)
            .build();

      assertEquals(BoundedOffHeapDataContainer.class, this.dataContainerFactory.construct(COMPONENT_NAME).getClass());
   }

   @Test
   public void testEvictionRemoveSegmentedOffHeap() {
      dataContainerFactory.configuration = new ConfigurationBuilder().clustering()
            .memory().storage(StorageType.OFF_HEAP).whenFull(EvictionStrategy.REMOVE).maxCount(1000)
            .clustering().cacheMode(CacheMode.DIST_ASYNC).build();

      Object component = dataContainerFactory.construct(COMPONENT_NAME);
      assertEquals(SegmentedBoundedOffHeapDataContainer.class, component.getClass());
   }
}
