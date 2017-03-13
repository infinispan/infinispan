package org.infinispan.server.test.cs.custom;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import javax.management.ObjectName;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.arquillian.utils.MBeanServerConnectionProvider;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.persistence.cluster.MyCustomCacheStore;
import org.infinispan.persistence.spi.ExternalStore;
import org.infinispan.server.infinispan.spi.InfinispanSubsystem;
import org.infinispan.server.test.category.CacheStore;
import org.infinispan.server.test.category.SingleNode;
import org.infinispan.server.test.util.ITestUtils;
import org.infinispan.server.test.util.ManagementClient;
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
@Category({SingleNode.class})
public class CustomCacheStoreIT {
   static final Log log = LogFactory.getLog(CustomCacheStoreIT.class);
   static final String TEST_CACHE = "customcs-cache";
   static final String TARGET_CONTAINER = "local";
   static final String CONFIG_TEMPLATE = "customcs-cache-configuration";

   @InfinispanResource("container1")
   RemoteInfinispanServer server;

   final int managementPort = 9990;
   final String cacheLoaderMBean = "jboss." + InfinispanSubsystem.SUBSYSTEM_NAME + ":type=Cache,name=\""+ TEST_CACHE +"(local)\",manager=\"local\",component=CacheLoader";


   @BeforeClass
   public static void beforeClass() throws Exception {
      String serverDir = System.getProperty("server1.dist");
      JavaArchive deployedCacheStore = ShrinkWrap.create(JavaArchive.class);
      deployedCacheStore.addPackage(MyCustomCacheStore.class.getPackage());
      deployedCacheStore.addAsServiceProvider(ExternalStore.class, MyCustomCacheStore.class);

      deployedCacheStore.as(ZipExporter.class).exportTo(
              new File(serverDir, "/standalone/deployments/custom-store.jar"), true);

      Map<String, String> props = new HashMap<>();
      props.put("customProperty", "10");
      final String cachestoreClass = "org.infinispan.persistence.cluster.MyCustomCacheStore";

      ManagementClient client = ManagementClient.getStandaloneInstance();
      client.addCacheConfiguration(CONFIG_TEMPLATE, TARGET_CONTAINER, ManagementClient.CacheTemplate.LOCAL);
      client.addCustomCacheStore(TARGET_CONTAINER, CONFIG_TEMPLATE, ManagementClient.CacheTemplate.LOCAL, "customcs", cachestoreClass, props);
      client.addCache(TEST_CACHE, TARGET_CONTAINER, CONFIG_TEMPLATE, ManagementClient.CacheType.LOCAL);
      //Requires reload - ISPN-7609
      client.reload();
   }

   @AfterClass
   public static void afterClass() throws Exception {
      String serverDir = System.getProperty("server1.dist");
      File jar = new File(serverDir, "/standalone/deployments/custom-store.jar");
      if (jar.exists())
         jar.delete();

      File f = new File(serverDir, "/standalone/deployments/custom-store.jar.deployed");
      if (f.exists())
         f.delete();

      ManagementClient client = ManagementClient.getStandaloneInstance();
      client.removeCache(TEST_CACHE, TARGET_CONTAINER, ManagementClient.CacheType.LOCAL);
      client.removeCacheConfiguration(CONFIG_TEMPLATE, TARGET_CONTAINER, ManagementClient.CacheTemplate.LOCAL);
   }

   @Test
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
