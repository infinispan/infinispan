package org.infinispan.server.test.cs.custom;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.File;

import javax.management.ObjectName;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.arquillian.core.RunningServer;
import org.infinispan.arquillian.core.WithRunningServer;
import org.infinispan.arquillian.utils.MBeanServerConnectionProvider;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.persistence.cluster.MyCustomCacheStore;
import org.infinispan.persistence.spi.ExternalStore;
import org.infinispan.server.infinispan.spi.InfinispanSubsystem;
import org.infinispan.server.test.category.CacheStore;
import org.infinispan.server.test.util.ITestUtils;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.exporter.ZipExporter;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 * Tests Deployeable Cache Stores which are placed into server deployments directory.
 *
 * @author <a href="mailto:jmarkos@redhat.com">Jakub Markos</a>
 * @author Sebastian Laskawiec
 */
@RunWith(Arquillian.class)
@Category({CacheStore.class})
public class CustomCacheStoreIT {

   private static File deployment;

   @InfinispanResource("standalone-customcs")
   RemoteInfinispanServer server;

   private static final int managementPort = 9990;

   private static final String cacheLoaderMBean = "jboss." + InfinispanSubsystem.SUBSYSTEM_NAME + ":type=Cache,name=\"default(local)\",manager=\"local\",component=CacheLoader";

   @BeforeClass
   public static void before() throws Exception {
      JavaArchive deployedCacheStore = ShrinkWrap.create(JavaArchive.class);
      deployedCacheStore.addPackage(MyCustomCacheStore.class.getPackage());
      deployedCacheStore.addAsServiceProvider(ExternalStore.class, MyCustomCacheStore.class);

      deployment = new File(System.getProperty("server1.dist"), "/standalone/deployments/custom-store.jar");
      deployedCacheStore.as(ZipExporter.class).exportTo(deployment, true);
   }

   @AfterClass
   public static void after() {
      if (deployment != null) {
         deployment.delete();
      }
   }

   @Test
   @WithRunningServer({@RunningServer(name = "standalone-customcs")})
   public void testIfDeployedCacheContainsProperValues() throws Exception {
      RemoteCacheManager rcm = ITestUtils.createCacheManager(server);
      RemoteCache<String, String> rc = rcm.getCache();
      assertNull(rc.get("key1"));
      rc.put("key1", "value1");
      assertEquals("value1", rc.get("key1"));
      // check via jmx that MyCustomCacheStore is indeed used
      MBeanServerConnectionProvider provider = new MBeanServerConnectionProvider(server.getHotrodEndpoint().getInetAddress().getHostName(), managementPort);
      assertEquals("[org.infinispan.persistence.cluster.MyCustomCacheStore]", getAttribute(provider, cacheLoaderMBean, "stores"));
   }

   private String getAttribute(MBeanServerConnectionProvider provider, String mbean, String attr) throws Exception {
      return provider.getConnection().getAttribute(new ObjectName(mbean), attr).toString();
   }
}
