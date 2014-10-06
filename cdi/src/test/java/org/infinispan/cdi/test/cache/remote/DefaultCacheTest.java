package org.infinispan.cdi.test.cache.remote;

import org.infinispan.cdi.Remote;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.core.test.ServerTestingUtil;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.testng.Arquillian;
import org.jboss.shrinkwrap.api.Archive;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;

import static org.infinispan.cdi.test.testutil.Deployments.baseDeployment;
import static org.infinispan.client.hotrod.TestHelper.startHotRodServer;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.Assert.assertEquals;

/**
 * Tests the default cache injection.
 *
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 */
@Test(groups = "functional", testName = "cdi.test.cache.remote.DefaultCacheTest")
public class DefaultCacheTest extends Arquillian {

   private static final String SERVER_LIST_KEY = "infinispan.client.hotrod.server_list";

   @Deployment
   public static Archive<?> deployment() {
      return baseDeployment()
            .addClass(DefaultCacheTest.class);
   }

   private static HotRodServer hotRodServer;
   private static EmbeddedCacheManager embeddedCacheManager;

   @Inject
   @Remote
   private RemoteCache<String, String> cache;

   @Inject
   private RemoteCache<String, String> remoteCache;

   @BeforeTest
   public void beforeMethod() {
      embeddedCacheManager = TestCacheManagerFactory.createCacheManager(
            hotRodCacheConfiguration(TestCacheManagerFactory
                  .getDefaultCacheConfiguration(false)));
      hotRodServer = startHotRodServer(embeddedCacheManager);
   }

   @AfterTest(alwaysRun = true)
   public void afterMethod() {
      TestingUtil.killCacheManagers(embeddedCacheManager);
      ServerTestingUtil.killServer(hotRodServer);
   }

   public void testDefaultCache() {
      cache.put("pete", "British");
      cache.put("manik", "Sri Lankan");

      assertEquals(cache.get("pete"), "British");
      assertEquals(cache.get("manik"), "Sri Lankan");
      assertEquals(remoteCache.get("pete"), "British");
      assertEquals(remoteCache.get("manik"), "Sri Lankan");
   }

   /**
    * Overrides the default remote cache manager.
    */
   @Produces
   @ApplicationScoped
   public static RemoteCacheManager defaultRemoteCacheManager() {
      return new RemoteCacheManager(
            new org.infinispan.client.hotrod.configuration.ConfigurationBuilder()
                  .addServers("127.0.0.1:" + hotRodServer.getPort()).build());
   }

}
