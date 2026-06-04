package org.infinispan.globalstate;

import static org.infinispan.testing.Testing.tmpDirectory;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Method;

import org.infinispan.commons.util.Util;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.container.impl.SharedContainerMaps;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.TransportFlags;
import org.testng.annotations.Test;

/**
 * Tests for runtime global configuration attribute updates.
 *
 * @since 16.2
 */
@Test(testName = "globalstate.GlobalConfigurationUpdateTest", groups = "functional")
public class GlobalConfigurationUpdateTest extends AbstractInfinispanTest {

   public void testUpdateAccurateSizeLocally(Method m) {
      String state = tmpDirectory(getClass().getSimpleName(), m.getName());
      GlobalConfigurationBuilder global = statefulGlobalBuilder(state, true);
      EmbeddedCacheManager cm = TestCacheManagerFactory.createClusteredCacheManager(true, global, null, new TransportFlags());
      try {
         cm.start();

         GlobalConfiguration globalConfig = cm.getCacheManagerConfiguration();
         assertFalse(globalConfig.metrics().accurateSize());

         cm.administration().updateGlobalConfigurationAttribute("metrics.accurate-size", "true");

         assertTrue(globalConfig.metrics().accurateSize());
      } finally {
         TestingUtil.killCacheManagers(cm);
      }
   }

   public void testUpdatePropagatesClusterWide(Method m) {
      String state1 = tmpDirectory(getClass().getSimpleName(), m.getName() + "1");
      GlobalConfigurationBuilder global1 = statefulGlobalBuilder(state1, true);
      String state2 = tmpDirectory(getClass().getSimpleName(), m.getName() + "2");
      GlobalConfigurationBuilder global2 = statefulGlobalBuilder(state2, true);
      EmbeddedCacheManager cm1 = TestCacheManagerFactory.createClusteredCacheManager(true, global1, null, new TransportFlags());
      EmbeddedCacheManager cm2 = TestCacheManagerFactory.createClusteredCacheManager(true, global2, null, new TransportFlags());
      try {
         cm1.start();
         cm2.start();

         assertFalse(cm1.getCacheManagerConfiguration().metrics().accurateSize());
         assertFalse(cm2.getCacheManagerConfiguration().metrics().accurateSize());

         cm1.administration().updateGlobalConfigurationAttribute("metrics.accurate-size", "true");

         eventually(() -> cm2.getCacheManagerConfiguration().metrics().accurateSize());
         assertTrue(cm1.getCacheManagerConfiguration().metrics().accurateSize());
         assertTrue(cm2.getCacheManagerConfiguration().metrics().accurateSize());
      } finally {
         TestingUtil.killCacheManagers(cm1, cm2);
      }
   }

   public void testUpdateImmutableAttributeThrows(Method m) {
      String state = tmpDirectory(getClass().getSimpleName(), m.getName());
      GlobalConfigurationBuilder global = statefulGlobalBuilder(state, true);
      EmbeddedCacheManager cm = TestCacheManagerFactory.createClusteredCacheManager(true, global, null, new TransportFlags());
      try {
         cm.start();

         assertThrows(IllegalArgumentException.class,
               () -> cm.administration().updateGlobalConfigurationAttribute("metrics.gauges", "false"));
      } finally {
         TestingUtil.killCacheManagers(cm);
      }
   }

   public void testUpdateInvalidAttributeThrows(Method m) {
      String state = tmpDirectory(getClass().getSimpleName(), m.getName());
      GlobalConfigurationBuilder global = statefulGlobalBuilder(state, true);
      EmbeddedCacheManager cm = TestCacheManagerFactory.createClusteredCacheManager(true, global, null, new TransportFlags());
      try {
         cm.start();

         assertThrows(IllegalArgumentException.class,
               () -> cm.administration().updateGlobalConfigurationAttribute("metrics.nonexistent", "true"));
      } finally {
         TestingUtil.killCacheManagers(cm);
      }
   }

   public void testJoiningNodePicksUpUpdate(Method m) {
      String state1 = tmpDirectory(getClass().getSimpleName(), m.getName() + "1");
      GlobalConfigurationBuilder global1 = statefulGlobalBuilder(state1, true);
      EmbeddedCacheManager cm1 = TestCacheManagerFactory.createClusteredCacheManager(true, global1, null, new TransportFlags());
      try {
         cm1.start();

         cm1.administration().updateGlobalConfigurationAttribute("metrics.accurate-size", "true");
         assertTrue(cm1.getCacheManagerConfiguration().metrics().accurateSize());

         String state2 = tmpDirectory(getClass().getSimpleName(), m.getName() + "2");
         GlobalConfigurationBuilder global2 = statefulGlobalBuilder(state2, true);
         EmbeddedCacheManager cm2 = TestCacheManagerFactory.createClusteredCacheManager(true, global2, null, new TransportFlags());
         try {
            cm2.start();

            eventually(() -> cm2.getCacheManagerConfiguration().metrics().accurateSize());
         } finally {
            TestingUtil.killCacheManagers(cm2);
         }
      } finally {
         TestingUtil.killCacheManagers(cm1);
      }
   }

   public void testToggleBackAndForth(Method m) {
      String state = tmpDirectory(getClass().getSimpleName(), m.getName());
      GlobalConfigurationBuilder global = statefulGlobalBuilder(state, true);
      EmbeddedCacheManager cm = TestCacheManagerFactory.createClusteredCacheManager(true, global, null, new TransportFlags());
      try {
         cm.start();

         GlobalConfiguration globalConfig = cm.getCacheManagerConfiguration();
         assertFalse(globalConfig.metrics().accurateSize());

         cm.administration().updateGlobalConfigurationAttribute("metrics.accurate-size", "true");
         assertTrue(globalConfig.metrics().accurateSize());

         cm.administration().updateGlobalConfigurationAttribute("metrics.accurate-size", "false");
         assertFalse(globalConfig.metrics().accurateSize());
      } finally {
         TestingUtil.killCacheManagers(cm);
      }
   }

   private static final String COUNT_CONTAINER = "count-pool";
   private static final String SIZE_CONTAINER = "size-pool";

   public void testUpdateContainerMemoryMaxCount(Method m) {
      String state = tmpDirectory(getClass().getSimpleName(), m.getName());
      GlobalConfigurationBuilder global = statefulGlobalBuilder(state, true);
      global.containerMemoryConfiguration(COUNT_CONTAINER).maxCount(100);
      EmbeddedCacheManager cm = TestCacheManagerFactory.createClusteredCacheManager(true, global, null, new TransportFlags());
      try {
         cm.start();

         GlobalConfiguration globalConfig = cm.getCacheManagerConfiguration();
         assertEquals(100L, globalConfig.getMemoryContainer().get(COUNT_CONTAINER).maxCount());

         SharedContainerMaps sharedContainerMaps = TestingUtil.extractGlobalComponent(cm, SharedContainerMaps.class);
         assertEquals(100L, sharedContainerMaps.getMap(COUNT_CONTAINER).capacity());

         cm.administration().updateGlobalConfigurationAttribute("container-memory." + COUNT_CONTAINER + ".max-count", "200");

         assertEquals(200L, globalConfig.getMemoryContainer().get(COUNT_CONTAINER).maxCount());
         assertEquals(200L, sharedContainerMaps.getMap(COUNT_CONTAINER).capacity());
      } finally {
         TestingUtil.killCacheManagers(cm);
      }
   }

   public void testUpdateContainerMemoryMaxSize(Method m) {
      String state = tmpDirectory(getClass().getSimpleName(), m.getName());
      GlobalConfigurationBuilder global = statefulGlobalBuilder(state, true);
      global.containerMemoryConfiguration(SIZE_CONTAINER).maxSize("1MB");
      EmbeddedCacheManager cm = TestCacheManagerFactory.createClusteredCacheManager(true, global, null, new TransportFlags());
      try {
         cm.start();

         GlobalConfiguration globalConfig = cm.getCacheManagerConfiguration();
         assertEquals("1MB", globalConfig.getMemoryContainer().get(SIZE_CONTAINER).maxSize());

         SharedContainerMaps sharedContainerMaps = TestingUtil.extractGlobalComponent(cm, SharedContainerMaps.class);
         long originalCapacity = sharedContainerMaps.getMap(SIZE_CONTAINER).capacity();

         cm.administration().updateGlobalConfigurationAttribute("container-memory." + SIZE_CONTAINER + ".max-size", "2MB");

         assertEquals("2MB", globalConfig.getMemoryContainer().get(SIZE_CONTAINER).maxSize());
         assertEquals(originalCapacity * 2, sharedContainerMaps.getMap(SIZE_CONTAINER).capacity());
      } finally {
         TestingUtil.killCacheManagers(cm);
      }
   }

   public void testUpdateContainerMemoryMaxCountPropagatesClusterWide(Method m) {
      String state1 = tmpDirectory(getClass().getSimpleName(), m.getName() + "1");
      GlobalConfigurationBuilder global1 = statefulGlobalBuilder(state1, true);
      global1.containerMemoryConfiguration(COUNT_CONTAINER).maxCount(100);
      String state2 = tmpDirectory(getClass().getSimpleName(), m.getName() + "2");
      GlobalConfigurationBuilder global2 = statefulGlobalBuilder(state2, true);
      global2.containerMemoryConfiguration(COUNT_CONTAINER).maxCount(100);
      EmbeddedCacheManager cm1 = TestCacheManagerFactory.createClusteredCacheManager(true, global1, null, new TransportFlags());
      EmbeddedCacheManager cm2 = TestCacheManagerFactory.createClusteredCacheManager(true, global2, null, new TransportFlags());
      try {
         cm1.start();
         cm2.start();

         cm1.administration().updateGlobalConfigurationAttribute("container-memory." + COUNT_CONTAINER + ".max-count", "200");

         SharedContainerMaps sharedContainerMaps2 = TestingUtil.extractGlobalComponent(cm2, SharedContainerMaps.class);
         eventually(() -> cm2.getCacheManagerConfiguration().getMemoryContainer().get(COUNT_CONTAINER).maxCount() == 200L);
         assertEquals(200L, sharedContainerMaps2.getMap(COUNT_CONTAINER).capacity());
      } finally {
         TestingUtil.killCacheManagers(cm1, cm2);
      }
   }

   public void testUpdateContainerMemoryMaxSizePropagatesClusterWide(Method m) {
      String state1 = tmpDirectory(getClass().getSimpleName(), m.getName() + "1");
      GlobalConfigurationBuilder global1 = statefulGlobalBuilder(state1, true);
      global1.containerMemoryConfiguration(SIZE_CONTAINER).maxSize("1MB");
      String state2 = tmpDirectory(getClass().getSimpleName(), m.getName() + "2");
      GlobalConfigurationBuilder global2 = statefulGlobalBuilder(state2, true);
      global2.containerMemoryConfiguration(SIZE_CONTAINER).maxSize("1MB");
      EmbeddedCacheManager cm1 = TestCacheManagerFactory.createClusteredCacheManager(true, global1, null, new TransportFlags());
      EmbeddedCacheManager cm2 = TestCacheManagerFactory.createClusteredCacheManager(true, global2, null, new TransportFlags());
      try {
         cm1.start();
         cm2.start();

         cm1.administration().updateGlobalConfigurationAttribute("container-memory." + SIZE_CONTAINER + ".max-size", "2MB");

         eventually(() -> "2MB".equals(cm2.getCacheManagerConfiguration().getMemoryContainer().get(SIZE_CONTAINER).maxSize()));
         assertEquals("2MB", cm1.getCacheManagerConfiguration().getMemoryContainer().get(SIZE_CONTAINER).maxSize());
      } finally {
         TestingUtil.killCacheManagers(cm1, cm2);
      }
   }

   public void testUpdateContainerMemoryInvalidContainerThrows(Method m) {
      String state = tmpDirectory(getClass().getSimpleName(), m.getName());
      GlobalConfigurationBuilder global = statefulGlobalBuilder(state, true);
      global.containerMemoryConfiguration(COUNT_CONTAINER).maxCount(100);
      EmbeddedCacheManager cm = TestCacheManagerFactory.createClusteredCacheManager(true, global, null, new TransportFlags());
      try {
         cm.start();

         assertThrows(IllegalArgumentException.class,
               () -> cm.administration().updateGlobalConfigurationAttribute("container-memory.nonexistent.max-count", "200"));
      } finally {
         TestingUtil.killCacheManagers(cm);
      }
   }

   private GlobalConfigurationBuilder statefulGlobalBuilder(String stateDirectory, boolean clear) {
      if (clear) Util.recursiveFileRemove(stateDirectory);
      GlobalConfigurationBuilder global = GlobalConfigurationBuilder.defaultClusteredBuilder();
      global.globalState().enable()
            .persistentLocation(stateDirectory)
            .sharedPersistentLocation(stateDirectory)
            .configurationStorage(ConfigurationStorage.OVERLAY);
      return global;
   }
}
