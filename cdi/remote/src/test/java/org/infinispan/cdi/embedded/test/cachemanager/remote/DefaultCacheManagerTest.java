package org.infinispan.cdi.embedded.test.cachemanager.remote;

import static org.testng.Assert.assertEquals;

import java.util.Properties;

import javax.inject.Inject;

import org.infinispan.cdi.embedded.test.Deployments;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.test.fwk.TestResourceTrackingListener;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.testng.Arquillian;
import org.jboss.shrinkwrap.api.Archive;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

/**
 * Test the default remote cache manager injection.
 *
 * @author Kevin Pollet &lt;kevin.pollet@serli.com&gt; (C) 2011 SERLI
 */
@Listeners(TestResourceTrackingListener.class)
@Test(groups = "functional", testName = "cdi.test.cachemanager.remote.DefaultCacheManagerTest")
public class DefaultCacheManagerTest extends Arquillian {

   private static final String SERVER_LIST_KEY = "infinispan.client.hotrod.server_list";
   private static final String DEFAULT_SERVER_LIST_VALUE = "127.0.0.1:11222";

   @Deployment
   public static Archive<?> deployment() {
      return Deployments.baseDeployment()
            .addClass(DefaultCacheManagerTest.class);
   }

   @Inject
   private RemoteCacheManager remoteCacheManager;

   public void testDefaultRemoteCacheManagerInjection() {
      final Properties properties = remoteCacheManager.getConfiguration().properties();

      assertEquals(properties.getProperty(SERVER_LIST_KEY), DEFAULT_SERVER_LIST_VALUE);
   }
}
