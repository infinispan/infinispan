package org.infinispan.cdi.embedded.test.cache.remote;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.startHotRodServer;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.Assert.assertEquals;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;

import org.infinispan.cdi.embedded.test.Deployments;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.TestResourceTrackingListener;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.testng.Arquillian;
import org.jboss.shrinkwrap.api.Archive;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

/**
 * Tests that the use of a specific cache manager for one cache.
 *
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 */
@Listeners(TestResourceTrackingListener.class)
@Test(groups = "functional", testName = "cdi.test.cache.remote.SpecificCacheManagerTest")
public class SpecificCacheManagerTest extends Arquillian {

   private static final String SERVER_LIST_KEY = "infinispan.client.hotrod.server_list";

   @Deployment
   public static Archive<?> deployment() {
      return Deployments.baseDeployment()
            .addClass(SpecificCacheManagerTest.class)
            .addClass(Small.class);
   }

   private static HotRodServer hotRodServer;
   private static EmbeddedCacheManager embeddedCacheManager;

   @Inject
   @Small
   private RemoteCache<String, String> cache;

   @BeforeClass
   public void beforeMethod() {
      embeddedCacheManager = TestCacheManagerFactory.createCacheManager(
            hotRodCacheConfiguration(TestCacheManagerFactory
                  .getDefaultCacheConfiguration(false)));
      embeddedCacheManager.defineConfiguration("small", embeddedCacheManager.getDefaultCacheConfiguration());
      embeddedCacheManager.getCache("small");
      hotRodServer = startHotRodServer(embeddedCacheManager);
   }

   @AfterClass(alwaysRun = true)
   public void afterMethod() {
      if (hotRodServer != null) hotRodServer.stop();
      if (embeddedCacheManager != null) embeddedCacheManager.stop();
   }

   public void testSpecificCacheManager() {
      cache.put("pete", "British");
      cache.put("manik", "Sri Lankan");

      assertEquals(cache.getName(), "small");
      assertEquals(cache.get("pete"), "British");
      assertEquals(cache.get("manik"), "Sri Lankan");
   }

   /**
    * Produces a specific cache manager for the small cache.
    *
    * @see Small
    */
   @Small
   @Produces
   @ApplicationScoped
   public static RemoteCacheManager smallRemoteCacheManager() {
      return new RemoteCacheManager(
            new org.infinispan.client.hotrod.configuration.ConfigurationBuilder()
                  .addServers("127.0.0.1:" + hotRodServer.getPort()).build());
   }

}
