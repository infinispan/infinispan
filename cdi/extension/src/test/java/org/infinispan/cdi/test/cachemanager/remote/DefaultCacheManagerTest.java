package org.infinispan.cdi.test.cachemanager.remote;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.testng.Arquillian;
import org.jboss.shrinkwrap.api.Archive;
import org.testng.annotations.Test;

import javax.inject.Inject;
import java.util.Properties;

import static org.infinispan.cdi.test.testutil.Deployments.baseDeployment;
import static org.testng.Assert.assertEquals;

/**
 * Test the default remote cache manager injection.
 *
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 */
@Test(groups = "functional", testName = "cdi.test.cachemanager.remote.DefaultCacheManagerTest")
public class DefaultCacheManagerTest extends Arquillian {

   private static final String SERVER_LIST_KEY = "infinispan.client.hotrod.server_list";
   private static final String DEFAULT_SERVER_LIST_VALUE = "127.0.0.1:11222";

   @Deployment
   public static Archive<?> deployment() {
      return baseDeployment()
            .addClass(DefaultCacheManagerTest.class);
   }

   @Inject
   private RemoteCacheManager remoteCacheManager;

   public void testDefaultRemoteCacheManagerInjection() {
      final Properties properties = remoteCacheManager.getProperties();

      assertEquals(properties.getProperty(SERVER_LIST_KEY), DEFAULT_SERVER_LIST_VALUE);
   }
}
