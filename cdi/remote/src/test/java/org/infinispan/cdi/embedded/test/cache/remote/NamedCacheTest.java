package org.infinispan.cdi.embedded.test.cache.remote;

import static org.infinispan.cdi.embedded.test.Deployments.baseDeployment;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.startHotRodServer;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.Assert.assertEquals;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;

import org.infinispan.cdi.remote.Remote;
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
 * Tests the named cache injection.
 *
 * @author Kevin Pollet &lt;kevin.pollet@serli.com&gt; (C) 2011 SERLI
 */
@Listeners(TestResourceTrackingListener.class)
@Test(groups = {"functional", "smoke"}, testName = "cdi.test.cache.remote.NamedCacheTest")
public class NamedCacheTest extends Arquillian {

   private static final String SERVER_LIST_KEY = "infinispan.client.hotrod.server_list";

   @Deployment
   public static Archive<?> deployment() {
      return baseDeployment()
            .addClass(NamedCacheTest.class)
            .addClass(Small.class);
   }

   private static HotRodServer hotRodServer;
   private static EmbeddedCacheManager embeddedCacheManager;

   @Inject
   @Remote("small")
   private RemoteCache<String, String> cache;

   @Inject
   @Small
   private RemoteCache<String, String> cacheWithQualifier;

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

   public void testNamedCache() {
      cache.put("pete", "British");
      cache.put("manik", "Sri Lankan");

      assertEquals(cache.getName(), "small");
      assertEquals(cache.get("pete"), "British");
      assertEquals(cache.get("manik"), "Sri Lankan");

      // here we check that the cache injection with the @Small qualifier works
      // like the injection with the @Remote qualifier

      assertEquals(cacheWithQualifier.getName(), "small");
      assertEquals(cacheWithQualifier.get("pete"), "British");
      assertEquals(cacheWithQualifier.get("manik"), "Sri Lankan");
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
