package org.infinispan.cdi.embedded.test.cachemanager.remote;

import static org.testng.Assert.assertEquals;

import java.util.Properties;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Disposes;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;

import org.infinispan.cdi.embedded.test.Deployments;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.test.fwk.TestResourceTrackingListener;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.testng.Arquillian;
import org.jboss.shrinkwrap.api.Archive;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

/**
 * Tests that the default remote cache manager can be overridden.
 *
 * @author Kevin Pollet &lt;kevin.pollet@serli.com&gt; (C) 2011 SERLI
 */
@Listeners(TestResourceTrackingListener.class)
@Test(groups = "functional", testName = "cdi.test.cachemanager.remote.DefaultCacheManagerOverrideTest")
public class DefaultCacheManagerOverrideTest extends Arquillian {

   private static final String SERVER_LIST_KEY = "infinispan.client.hotrod.server_list";
   private static final String SERVER_LIST_VALUE = "foo:15444";

   @Deployment
   public static Archive<?> deployment() {
      return Deployments.baseDeployment()
            .addClass(DefaultCacheManagerOverrideTest.class);
   }

   @Inject
   private RemoteCacheManager remoteCacheManager;

   public void testDefaultRemoteCacheManagerOverride() {
      final Properties properties = remoteCacheManager.getConfiguration().properties();

      assertEquals(properties.getProperty(SERVER_LIST_KEY), SERVER_LIST_VALUE);
   }

   /**
    * The default remote cache manager producer. This producer will override the default remote cache manager producer
    * provided by the CDI extension.
    */
   @Produces
   @ApplicationScoped
   public RemoteCacheManager defaultRemoteCacheManager() {
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder =
         HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      clientBuilder.addServers(SERVER_LIST_VALUE);
      return new RemoteCacheManager(clientBuilder.build());
   }

   public void stopRemoteCacheManager(@Disposes RemoteCacheManager remoteCacheManager) {
      remoteCacheManager.stop();
   }
}
