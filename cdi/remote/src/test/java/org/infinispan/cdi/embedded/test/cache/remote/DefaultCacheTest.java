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
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.core.test.ServerTestingUtil;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.TestingUtil;
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
 * Tests the default cache injection.
 *
 * @author Kevin Pollet &lt;kevin.pollet@serli.com&gt; (C) 2011 SERLI
 */
@Listeners(TestResourceTrackingListener.class)
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

   @BeforeClass
   public void beforeMethod() {
      embeddedCacheManager = TestCacheManagerFactory.createCacheManager(
            hotRodCacheConfiguration(TestCacheManagerFactory
                  .getDefaultCacheConfiguration(false)));
      hotRodServer = startHotRodServer(embeddedCacheManager);
   }

   @AfterClass(alwaysRun = true)
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
         HotRodClientTestingUtil.newRemoteConfigurationBuilder(hotRodServer).build());
   }

}
