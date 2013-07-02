package org.infinispan.cdi.test.cachemanager.remote;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.testng.Arquillian;
import org.jboss.shrinkwrap.api.Archive;
import org.testng.annotations.Test;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;
import java.util.Properties;

import static org.infinispan.cdi.test.testutil.Deployments.baseDeployment;
import static org.testng.Assert.assertEquals;

/**
 * Tests that the default remote cache manager can be overridden.
 *
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 */
@Test(groups = "functional", testName = "cdi.test.cachemanager.remote.DefaultCacheManagerOverrideTest")
public class DefaultCacheManagerOverrideTest extends Arquillian {

   private static final String SERVER_LIST_KEY = "infinispan.client.hotrod.server_list";
   private static final String SERVER_LIST_VALUE = "foo:15444";

   @Deployment
   public static Archive<?> deployment() {
      return baseDeployment()
            .addClass(DefaultCacheManagerOverrideTest.class);
   }

   @Inject
   private RemoteCacheManager remoteCacheManager;

   public void testDefaultRemoteCacheManagerOverride() {
      final Properties properties = remoteCacheManager.getProperties();

      assertEquals(properties.getProperty(SERVER_LIST_KEY), SERVER_LIST_VALUE);
   }

   /**
    * The default remote cache manager producer. This producer will override the default remote cache manager producer
    * provided by the CDI extension.
    */
   @Produces
   @ApplicationScoped
   public RemoteCacheManager defaultRemoteCacheManager() {
      final Properties properties = new Properties();
      properties.put(SERVER_LIST_KEY, SERVER_LIST_VALUE);

      return new RemoteCacheManager(properties);
   }
}
