package org.infinispan.client.hotrod.admin;

import static org.infinispan.commons.test.CommonsTestingUtil.tmpDirectory;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.lang.reflect.Method;

import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.internal.PrivateGlobalConfigurationBuilder;
import org.infinispan.globalstate.ConfigurationStorage;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.core.admin.embeddedserver.EmbeddedServerAdminOperationHandler;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "client.hotrod.admin.RemoteCacheAdminPermanentTest")
public class RemoteCacheAdminPermanentTest extends MultiHotRodServersTest {
   char serverId;
   boolean clear = true;

   @Override
   protected void createCacheManagers() throws Throwable {
      serverId = 'A';
      ConfigurationBuilder builder = hotRodCacheConfiguration(
            getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false));
      createHotRodServers(2, builder);
   }

   @Override
   protected HotRodServer addHotRodServer(ConfigurationBuilder builder) {
      return addStatefulHotRodServer(builder, serverId++);
   }

   protected boolean isShared() {
      return false;
   }

   protected HotRodServer addStatefulHotRodServer(ConfigurationBuilder builder, char id) {
      GlobalConfigurationBuilder gcb = GlobalConfigurationBuilder.defaultClusteredBuilder();
      gcb.addModule(PrivateGlobalConfigurationBuilder.class).serverMode(true);
      String stateDirectory = tmpDirectory(this.getClass().getSimpleName(), Character.toString(id));
      if (clear)
         Util.recursiveFileRemove(stateDirectory);
      gcb.globalState().enable().persistentLocation(stateDirectory).
         configurationStorage(ConfigurationStorage.OVERLAY);
      if (isShared()) {
         String sharedDirectory = tmpDirectory(this.getClass().getSimpleName(), "COMMON");
         gcb.globalState().sharedPersistentLocation(sharedDirectory);
      } else {
         gcb.globalState().sharedPersistentLocation(stateDirectory);
      }
      EmbeddedCacheManager cm = addClusterEnabledCacheManager(gcb, builder);
      cm.defineConfiguration("template", builder.build());
      HotRodServerConfigurationBuilder serverBuilder = new HotRodServerConfigurationBuilder();
      serverBuilder.adminOperationsHandler(new EmbeddedServerAdminOperationHandler());
      HotRodServer server = HotRodClientTestingUtil.startHotRodServer(cm, serverBuilder);
      servers.add(server);
      return server;
   }

   public void permanentCacheTest(Method m) throws Throwable {
      String cacheName = m.getName();
      client(0).administration().createCache(cacheName, "template");
      assertTrue(manager(0).cacheExists(cacheName));
      assertTrue(manager(1).cacheExists(cacheName));
      killAll();
      clear = false;
      createCacheManagers();
      assertTrue(manager(0).cacheExists(cacheName));
      client(0).administration().removeCache(cacheName);
      killAll();
      clear = false;
      createCacheManagers();
      assertFalse(manager(0).cacheExists(cacheName));
   }
}
