package org.infinispan.globalstate;

import static org.infinispan.commons.test.CommonsTestingUtil.tmpDirectory;
import static org.infinispan.commons.test.Exceptions.expectException;
import static org.testng.Assert.assertNotEquals;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.fail;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.lang.reflect.Method;
import java.util.Properties;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.commons.jdkspecific.CallerId;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.global.UncleanShutdownAction;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.manager.EmbeddedCacheManagerStartupException;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachemanagerlistener.annotation.ConfigurationChanged;
import org.infinispan.notifications.cachemanagerlistener.event.ConfigurationChangedEvent;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.TransportFlags;
import org.testng.annotations.Test;

/**
 * @since 9.2
 */
@Test(testName = "globalstate.GlobalStateTest", groups = "functional")
public class GlobalStateTest extends AbstractInfinispanTest {

   public void testReplicatedState(Method m) {
      String state1 = tmpDirectory(this.getClass().getSimpleName(), m.getName() + "1");
      GlobalConfigurationBuilder global1 = statefulGlobalBuilder(state1, true);
      String state2 = tmpDirectory(this.getClass().getSimpleName(), m.getName() + "2");
      GlobalConfigurationBuilder global2 = statefulGlobalBuilder(state2, true);
      EmbeddedCacheManager cm1 = TestCacheManagerFactory.createClusteredCacheManager(false, global1, null, new TransportFlags());
      EmbeddedCacheManager cm2 = TestCacheManagerFactory.createClusteredCacheManager(false, global2, null, new TransportFlags());
      try {
         Configuration cacheConfig = new ConfigurationBuilder().build();
         Configuration template = new ConfigurationBuilder().template(true).build();
         cm1.start();
         cm2.start();
         cm1.defineConfiguration("not-replicated-template", template);
         cm1.createCache("not-replicated-cache", cacheConfig);
         assertNull(cm2.getCacheConfiguration("not-replicated-template"));
         assertFalse(cm2.cacheExists("not-replicated-cache"));

         cm1.administration().getOrCreateCache("replicated-cache", cacheConfig);
         cm1.administration().getOrCreateTemplate("replicated-template", template);
         assertNotNull(cm2.getCache("replicated-cache"));
         assertNotNull(cm2.getCacheConfiguration("replicated-template"));

         assertEquals(2, cm1.getCacheNames().size());
         assertEquals(1, cm2.getCacheNames().size());
         cm1.stop();
         cm2.stop();

         global1 = statefulGlobalBuilder(state1, false);
         EmbeddedCacheManager newCm1 = TestCacheManagerFactory.createClusteredCacheManager(false, global1, new ConfigurationBuilder(), new TransportFlags());
         assertNotNull(newCm1.getCache("replicated-cache"));
         assertNotNull(newCm1.getCacheConfiguration("replicated-template"));
      } finally {
         TestingUtil.killCacheManagers(cm1, cm2);
      }
   }

   public void testLockPersistentLocation() {
      String name = CallerId.getCallerMethodName(1);
      String stateDirectory = tmpDirectory(name, "COMMON");

      GlobalConfigurationBuilder global1 = statefulGlobalBuilder(stateDirectory, true);
      GlobalConfigurationBuilder global2 = statefulGlobalBuilder(stateDirectory, true);

      EmbeddedCacheManager cm1 = TestCacheManagerFactory.createClusteredCacheManager(false, global1, new ConfigurationBuilder(), new TransportFlags());
      EmbeddedCacheManager cm2 = TestCacheManagerFactory.createClusteredCacheManager(false, global2, new ConfigurationBuilder(), new TransportFlags());
      try {
         cm1.start();
         expectException(EmbeddedCacheManagerStartupException.class, "ISPN000693:.*", cm2::start);
      } finally {
         TestingUtil.killCacheManagers(cm1, cm2);
      }
   }

   public void testCorruptGlobalState(Method m) throws Exception {
      String state1 = tmpDirectory(this.getClass().getSimpleName(), m.getName() + "1");
      GlobalConfigurationBuilder global1 = statefulGlobalBuilder(state1, true);
      String state2 = tmpDirectory(this.getClass().getSimpleName(), m.getName() + "2");
      GlobalConfigurationBuilder global2 = statefulGlobalBuilder(state2, true);
      EmbeddedCacheManager cm1 = TestCacheManagerFactory.createClusteredCacheManager(false, global1, new ConfigurationBuilder(), new TransportFlags());
      EmbeddedCacheManager cm2 = TestCacheManagerFactory.createClusteredCacheManager(false, global2, new ConfigurationBuilder(), new TransportFlags());
      try {
         cm1.start();
         cm2.start();
         cm1.stop();
         cm2.stop();
         // corrupt one of the state files
         Writer w = new FileWriter(new File(state1, ScopedPersistentState.GLOBAL_SCOPE + ".state"));
         w.write("'Cause I want to be anarchy\nIt's the only way to be");
         w.close();
         global1 = statefulGlobalBuilder(state1, false);
         EmbeddedCacheManager newCm1 = TestCacheManagerFactory.createClusteredCacheManager(false, global1, new ConfigurationBuilder(), new TransportFlags());
         expectException(EmbeddedCacheManagerStartupException.class, "ISPN000516: The state file for '___global' is invalid.*", newCm1::start);
      } finally {
         TestingUtil.killCacheManagers(cm1, cm2);
      }
   }

   public void testIncompatibleGlobalState(Method m) throws Exception {
      String state1 = tmpDirectory(this.getClass().getSimpleName(), m.getName() + "1");
      GlobalConfigurationBuilder global1 = statefulGlobalBuilder(state1, true);
      String state2 = tmpDirectory(this.getClass().getSimpleName(), m.getName() + "2");
      GlobalConfigurationBuilder global2 = statefulGlobalBuilder(state2, true);
      EmbeddedCacheManager cm1 = TestCacheManagerFactory.createClusteredCacheManager(false, global1, new ConfigurationBuilder(), new TransportFlags());
      EmbeddedCacheManager cm2 = TestCacheManagerFactory.createClusteredCacheManager(false, global2, new ConfigurationBuilder(), new TransportFlags());
      try {
         cm1.start();
         cm2.start();
         // Create two DIST caches
         ConfigurationBuilder distBuilder = new ConfigurationBuilder();
         distBuilder.clustering().cacheMode(CacheMode.DIST_SYNC);
         cm1.administration().createCache("cache1", distBuilder.build());
         cm1.administration().createCache("cache2", distBuilder.build());
         cm2.stop();

         // Remove the caches from the first node
         cm1.administration().removeCache("cache1");
         cm1.administration().removeCache("cache2");
         // Recreate the cache as REPL
         ConfigurationBuilder replBuilder = new ConfigurationBuilder();
         replBuilder.clustering().cacheMode(CacheMode.REPL_SYNC);
         cm1.administration().createCache("cache1", replBuilder.build());

         // Attempt to restart the second cache manager
         global2 = statefulGlobalBuilder(state2, false);
         EmbeddedCacheManager newCm2 = TestCacheManagerFactory.createClusteredCacheManager(false, global2, new ConfigurationBuilder(), new TransportFlags());
         expectException(EmbeddedCacheManagerStartupException.class,
                         "(?s)ISPN000500: Cannot create clustered configuration for cache.*",
               newCm2::start);
      } finally {
         TestingUtil.killCacheManagers(cm1, cm2);
      }
   }

   public void testConfigurationUpdate(Method m) {
      String state1 = tmpDirectory(this.getClass().getSimpleName(), m.getName() + "1");
      GlobalConfigurationBuilder global1 = statefulGlobalBuilder(state1, true);
      String state2 = tmpDirectory(this.getClass().getSimpleName(), m.getName() + "2");
      GlobalConfigurationBuilder global2 = statefulGlobalBuilder(state2, true);
      EmbeddedCacheManager cm1 = TestCacheManagerFactory.createClusteredCacheManager(false, global1, new ConfigurationBuilder(), new TransportFlags());
      EmbeddedCacheManager cm2 = TestCacheManagerFactory.createClusteredCacheManager(false, global2, new ConfigurationBuilder(), new TransportFlags());
      try {
         cm1.start();
         cm2.start();
         // Create two DIST caches
         ConfigurationBuilder builder = new ConfigurationBuilder();
         builder.clustering().cacheMode(CacheMode.DIST_SYNC).memory().maxCount(1000);
         cm1.administration().getOrCreateCache("cache1", builder.build());
         assertEquals(1000, cm1.getCache("cache1").getCacheConfiguration().memory().maxCount());
         assertEquals(1000, cm2.getCache("cache1").getCacheConfiguration().memory().maxCount());
         // Update the configuration
         builder.clustering().cacheMode(CacheMode.DIST_SYNC).memory().maxCount(2000);
         cm1.administration().withFlags(CacheContainerAdmin.AdminFlag.UPDATE).getOrCreateCache("cache1", builder.build());
         assertEquals(2000, cm1.getCache("cache1").getCacheConfiguration().memory().maxCount());
         assertEquals(2000, cm2.getCache("cache1").getCacheConfiguration().memory().maxCount());
         // Verify that it is unchanged if we don't use the UPDATE flag
         builder.clustering().cacheMode(CacheMode.DIST_SYNC).memory().maxCount(3000);
         cm1.administration().getOrCreateCache("cache1", builder.build());
         assertEquals(2000, cm1.getCache("cache1").getCacheConfiguration().memory().maxCount());
         assertEquals(2000, cm2.getCache("cache1").getCacheConfiguration().memory().maxCount());
         // Try to use an incompatible configuration
         builder.clustering().cacheMode(CacheMode.REPL_SYNC);
         Exceptions.expectRootCause(IllegalArgumentException.class, () -> cm1.administration().withFlags(CacheContainerAdmin.AdminFlag.UPDATE).getOrCreateCache("cache1", builder.build()));
      } finally {
         TestingUtil.killCacheManagers(cm1, cm2);
      }
   }

   private GlobalConfigurationBuilder statefulGlobalBuilder(String stateDirectory, boolean clear) {
      if (clear) Util.recursiveFileRemove(stateDirectory);
      GlobalConfigurationBuilder global = GlobalConfigurationBuilder.defaultClusteredBuilder();
      global.globalState().enable().persistentLocation(stateDirectory).sharedPersistentLocation(stateDirectory).configurationStorage(ConfigurationStorage.OVERLAY);
      return global;
   }

   public void testFailStartup(Method m) throws Exception {
      String state = tmpDirectory(this.getClass().getSimpleName(), m.getName());
      GlobalConfigurationBuilder global = statefulGlobalBuilder(state, true);
      global.transport().transport(new FailingJGroupsTransport());
      EmbeddedCacheManager cm = TestCacheManagerFactory.createClusteredCacheManager(false, global, new ConfigurationBuilder(), new TransportFlags());
      try {
         cm.start();
         fail("Should not reach here");
      } catch (Exception e) {
         // Ensure there is no global state file
         File globalStateFile = new File(state, ScopedPersistentState.GLOBAL_SCOPE + ".state");
         assertFalse(globalStateFile.exists());
      } finally {
         TestingUtil.killCacheManagers(cm);
      }
   }

   public void testUncleanShutdownAction() throws IOException {
      String state = tmpDirectory(this.getClass().getSimpleName(), CallerId.getCallerMethodName(1));

      // Test the default FAIL action by creating a "dangling" lock file
      GlobalConfigurationBuilder global = statefulGlobalBuilder(state, true);
      global.globalState().uncleanShutdownAction(UncleanShutdownAction.FAIL);
      File globalLockFile = new File(state, ScopedPersistentState.GLOBAL_SCOPE + ".lck");
      globalLockFile.getParentFile().mkdirs();
      globalLockFile.createNewFile();
      EmbeddedCacheManager cm = TestCacheManagerFactory.createClusteredCacheManager(false, global, null, new TransportFlags());
      Exceptions.expectException("ISPN000693:.*", cm::start, EmbeddedCacheManagerStartupException.class, CacheConfigurationException.class);


      // Remove the lock file, this should allow the cache manager to start
      globalLockFile.delete();
      cm = TestCacheManagerFactory.createClusteredCacheManager(false, global, null, new TransportFlags());
      try {
         cm.start();
      } finally {
         TestingUtil.killCacheManagers(cm);
      }

      File globalScopeFile = new File(state, ScopedPersistentState.GLOBAL_SCOPE + ".state");
      Properties properties = new Properties();
      try (FileReader reader = new FileReader(globalScopeFile)) {
         properties.load(reader);
      }
      String uuid = properties.getProperty("uuid");

      // Test the PURGE action by creating a dangling lock file
      globalLockFile.createNewFile();
      global = statefulGlobalBuilder(state, false);
      global.globalState().uncleanShutdownAction(UncleanShutdownAction.PURGE);
      cm = TestCacheManagerFactory.createClusteredCacheManager(false, global, null, new TransportFlags());
      try {
         cm.start();
      } finally {
         TestingUtil.killCacheManagers(cm);
      }
      properties.clear();
      try (FileReader reader = new FileReader(globalScopeFile)) {
         properties.load(reader);
      }
      assertNotEquals(uuid, properties.getProperty("uuid"), "uuids should be different");

      uuid = properties.getProperty("uuid");

      // Test the IGNORE action
      globalLockFile.createNewFile();
      global = statefulGlobalBuilder(state, false);
      global.globalState().uncleanShutdownAction(UncleanShutdownAction.IGNORE);
      cm = TestCacheManagerFactory.createClusteredCacheManager(false, global, null, new TransportFlags());
      try {
         cm.start();
      } finally {
         TestingUtil.killCacheManagers(cm);
      }
      properties.clear();
      try (FileReader reader = new FileReader(globalScopeFile)) {
         properties.load(reader);
      }
      assertEquals("uuids should be the same", uuid, properties.getProperty("uuid"));
   }

   public void testCacheManagerNotifications(Method m) {
      String state1 = tmpDirectory(this.getClass().getSimpleName(), m.getName() + "1");
      GlobalConfigurationBuilder global1 = statefulGlobalBuilder(state1, true);
      String state2 = tmpDirectory(this.getClass().getSimpleName(), m.getName() + "2");
      GlobalConfigurationBuilder global2 = statefulGlobalBuilder(state2, true);
      EmbeddedCacheManager cm1 = TestCacheManagerFactory.createClusteredCacheManager(false, global1, null, new TransportFlags());
      EmbeddedCacheManager cm2 = TestCacheManagerFactory.createClusteredCacheManager(false, global2, null, new TransportFlags());
      try {
         Configuration cacheConfig = new ConfigurationBuilder().build();
         Configuration template = new ConfigurationBuilder().template(true).build();
         cm1.start();
         cm2.start();
         cm1.addListener(new StateListener());
         cm2.addListener(new StateListener());

         cm1.administration().getOrCreateCache("replicated-cache", cacheConfig);
         cm1.administration().getOrCreateTemplate("replicated-template", template);
         assertNotNull(cm2.getCache("replicated-cache"));
         assertNotNull(cm2.getCacheConfiguration("replicated-template"));

         assertEquals(1, cm1.getCacheNames().size());
         assertEquals(1, cm2.getCacheNames().size());
         cm1.stop();
         cm2.stop();
      } finally {
         TestingUtil.killCacheManagers(cm1, cm2);
      }
   }

   public void testNameSize(Method m) {
      final String cacheName = new String(new char[256]);
      final String exceptionMessage = String.format("ISPN000663: Name must be less than 256 bytes, current name '%s' exceeds the size.", cacheName);
      String state1 = tmpDirectory(this.getClass().getSimpleName(), m.getName());
      GlobalConfigurationBuilder gcb = statefulGlobalBuilder(state1, true);
      EmbeddedCacheManager cm = TestCacheManagerFactory.createClusteredCacheManager(false, gcb, null, new TransportFlags());
      final Configuration configuration = new ConfigurationBuilder().build();

      try {
         cm.start();
         expectException(exceptionMessage,
               () -> cm.administration().getOrCreateCache(cacheName, configuration),
               CacheConfigurationException.class);
         expectException(exceptionMessage,
               () -> cm.administration().createTemplate(cacheName, configuration),
               CacheConfigurationException.class);
         expectException(exceptionMessage,
               () -> cm.administration().getOrCreateTemplate(cacheName, configuration),
               CacheConfigurationException.class);
      } finally {
         TestingUtil.killCacheManagers(cm);
      }
   }

   @Listener
   public static class StateListener {
      @ConfigurationChanged
      public void configurationChanged(ConfigurationChangedEvent event) {

      }
   }

   public static class FailingJGroupsTransport extends JGroupsTransport {

      @Override
      public void start() {
         throw new RuntimeException("fail");
      }
   }
}
