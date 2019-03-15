package org.infinispan.server.test.jcache;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.lang.management.ManagementFactory;
import java.util.stream.StreamSupport;
import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.cache.spi.CachingProvider;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.arquillian.core.RunningServer;
import org.infinispan.arquillian.core.WithRunningServer;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.jcache.RIMBeanServerRegistrationUtility.ObjectNameType;
import org.jboss.arquillian.junit.Arquillian;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * Test for issues related to ISPN-6574
 *
 * Test check whether JCache API is aware of caches defined in Infinispan server
 * configuration
 *
 * Three tested functionalities are not yet implemented (not part of the
 * original ticket), fix pending in <b>ISPN-7095</b>
 *
 * Calling getCache on a cache will refresh that cache inside CacheManager,
 * making the other functionality work ok for that particular cache (hence the
 * separate caches for each test)
 *
 * @author zhostasa
 *
 */
@RunWith(Arquillian.class)
@WithRunningServer({ @RunningServer(name = "cachecontainer") })
public class JCacheStateAwarenessIT {

   private static final String enableStatisticsTestCacheName = "enableStatisticsTestCache";
   private static final String enableManagementTestCacheName = "enableManagementTestCache";
   private static final String getCacheNamesTestCacheName = "getCacheNamesTestCache";
   private static final String getCacheTestCacheName = "getCacheTestCache";

   private static final String testKey = "testKey";
   private static final String testValue = "testValue";

   private static final String cachingProvider = "org.infinispan.jcache.remote.JCachingProvider";

   private static CachingProvider jcacheProvider;
   private static MBeanServer mBeanServer;

   @InfinispanResource("cachecontainer")
   private RemoteInfinispanServer server1;
   private RemoteCacheManager rcm1;
   private CacheManager cacheManager;

   @BeforeClass
   public static void setUpClass() {
      mBeanServer = ManagementFactory.getPlatformMBeanServer();

      jcacheProvider = Caching.getCachingProvider(cachingProvider);
   }

   @AfterClass
   public static void tearDownClass() {
      jcacheProvider.close();
   }

   @Before
   public void setUp() {
      Configuration conf = new ConfigurationBuilder().addServer()
            .host(server1.getHotrodEndpoint().getInetAddress().getHostName())
            .port(server1.getHotrodEndpoint().getPort()).build();

      rcm1 = new RemoteCacheManager(conf);

      cacheManager = jcacheProvider.getCacheManager();
   }

   @After
   public void tearDown() {
      if (rcm1 != null) {
         rcm1.stop();
      }
   }

   /**
    * Test enableStatistics command on JCache API <br>
    * <br>
    * Fix pending in <b>ISPN-7095</b>
    */
   @Test
   @Ignore("Ignored until ISPN-7095")
   public void testEnableStatistics() throws Exception {

      ObjectName mBeanName = getMBeanName(enableStatisticsTestCacheName, "Statistics");

      cacheManager.enableStatistics(enableStatisticsTestCacheName, true);

      assertTrue("Statistics register as disabled after enabling", mBeanServer.isRegistered(mBeanName));

      cacheManager.enableStatistics(enableStatisticsTestCacheName, false);

      assertFalse("Statistics register as enabled after disabling", mBeanServer.isRegistered(mBeanName));
   }

   /**
    * Test enableManagement command on JCache API<br>
    * <br>
    * Fix pending in <b>ISPN-7095</b>
    *
    * @throws MalformedObjectNameException
    */
   @Test
   @Ignore("Ignored until ISPN-7095")
   public void testEnableManagement() throws MalformedObjectNameException {

      ObjectName mBeanName = getMBeanName(enableManagementTestCacheName, "Configuration");

      cacheManager.enableManagement(enableManagementTestCacheName, true);

      assertTrue("Statistics register as disabled after enabling", mBeanServer.isRegistered(mBeanName));

      cacheManager.enableManagement(enableManagementTestCacheName, false);

      assertFalse("Statistics register as enabled after disabling", mBeanServer.isRegistered(mBeanName));

   }

   /**
    * Creates canonical name of managed bean
    *
    * @param testCacheName
    *           name of the cache to enable management on
    * @param objectNameType
    *           Object name as per {@link ObjectNameType}
    *
    * @return ObjectName of the managed bean
    * @throws MalformedObjectNameException
    */
   private ObjectName getMBeanName(String testCacheName, String objectNameType) throws MalformedObjectNameException {
      return new ObjectName("javax.cache:type=Cache" + objectNameType + ",CacheManager="
            + cacheManager.getURI().toString() + ",Cache=" + testCacheName);
   }

   /**
    * Test whether cache is found in cache name list of JCache API, fix
    * pending<br>
    * <br>
    * Fix pending in <b>ISPN-7095</b>
    */
   @Test
   @Ignore("Ignored until ISPN-7095")
   public void testGetCacheNames() {
      boolean passed = StreamSupport.stream(cacheManager.getCacheNames().spliterator(), true)
            .anyMatch(n -> getCacheNamesTestCacheName.equalsIgnoreCase(n));

      assertTrue(getCacheNamesTestCacheName + " cache name was not found in list retrieved from CacheManager", passed);
   }

   /**
    * Accesses cache trough HR client to confirm its existence and insert data,
    * then attempts to access it trough JCache API and retrieve the data
    *
    * @param testCacheName
    *           test on specific cache name, development feature
    */
   @Test
   public void testCacheGet(String testCacheName) {

      String cacheName = testCacheName != null ? testCacheName : getCacheTestCacheName;

      RemoteCache<Object, Object> remoteCacheFromHR = rcm1.getCache(cacheName);

      assertNotNull("The cache " + cacheName + " is not accessible trough HR (e.g. does not exist or is not there)",
            remoteCacheFromHR);

      remoteCacheFromHR.put(testKey, testValue);

      Cache<Object, Object> remoteCacheFromJCache = cacheManager.getCache(cacheName);

      assertNotNull("The cache " + cacheName + " is not accessible trough JCache", remoteCacheFromJCache);

      assertNotNull("Cache " + cacheName + " was retrieved, but key " + testKey + " could not be retrieved",
            remoteCacheFromJCache.get(testKey));

      assertEquals("The cache was retrieved but the retrieved value was not same", remoteCacheFromJCache.get(testKey),
            testValue);
   }
}
