package org.infinispan.jmx;

import static org.infinispan.test.TestingUtil.getCacheObjectName;
import static org.infinispan.test.TestingUtil.k;
import static org.infinispan.test.TestingUtil.v;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import javax.management.Attribute;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.infinispan.commons.jmx.MBeanServerLookup;
import org.infinispan.commons.jmx.TestMBeanServerLookup;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.persistence.impl.MarshalledEntryUtil;
import org.infinispan.persistence.dummy.DummyInMemoryStore;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/**
 * Tester class for ActivationInterceptor and PassivationInterceptor.
 *
 * @author Mircea.Markus@jboss.com
 * @author Galder Zamarreño
 */
@Test(groups = "functional", testName = "jmx.ActivationAndPassivationInterceptorMBeanTest")
public class ActivationAndPassivationInterceptorMBeanTest extends SingleCacheManagerTest {

   private static final String JMX_DOMAIN = ActivationAndPassivationInterceptorMBeanTest.class.getSimpleName();

   private final MBeanServerLookup mBeanServerLookup = TestMBeanServerLookup.create();

   private DummyInMemoryStore loader;
   private ObjectName activationInterceptorObjName;
   private ObjectName passivationInterceptorObjName;

   protected EmbeddedCacheManager createCacheManager() throws Exception {
      GlobalConfigurationBuilder globalBuilder = new GlobalConfigurationBuilder();
      globalBuilder.jmx().enabled(true)
                   .mBeanServerLookup(mBeanServerLookup)
                   .domain(JMX_DOMAIN);

      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.memory().maxCount(1)
            .statistics().enable()
            .persistence()
               .passivation(true)
               .addStore(DummyInMemoryStoreConfigurationBuilder.class);

      return TestCacheManagerFactory.createCacheManager(globalBuilder, builder);
   }

   @Override
   protected void setup() throws Exception {
      super.setup();
      activationInterceptorObjName =
            getCacheObjectName(JMX_DOMAIN, getDefaultCacheName() + "(local)", "Activation");
      passivationInterceptorObjName =
            getCacheObjectName(JMX_DOMAIN, getDefaultCacheName() + "(local)", "Passivation");
      loader = TestingUtil.getFirstStore(cache);
   }

   @AfterMethod
   public void resetStats() throws Exception {
      MBeanServer mBeanServer = mBeanServerLookup.getMBeanServer();
      mBeanServer.invoke(activationInterceptorObjName, "resetStatistics", new Object[0], new String[0]);
      mBeanServer.invoke(passivationInterceptorObjName, "resetStatistics", new Object[0], new String[0]);
   }

   public void passivateAll() throws Exception {
      mBeanServerLookup.getMBeanServer().invoke(passivationInterceptorObjName, "passivateAll", new Object[0], new String[0]);
   }

   public void testDisableStatistics() throws Exception {
      MBeanServer mBeanServer = mBeanServerLookup.getMBeanServer();
      mBeanServer.setAttribute(activationInterceptorObjName, new Attribute("StatisticsEnabled", Boolean.FALSE));
      assert mBeanServer.getAttribute(activationInterceptorObjName, "Activations").toString().equals("N/A");
      mBeanServer.setAttribute(activationInterceptorObjName, new Attribute("StatisticsEnabled", Boolean.TRUE));
   }

   public void testActivationOnGet(Method m) {
      assertActivationCount(0);
      assert cache.get(k(m)) == null;
      assertActivationCount(0);
      loader.write(MarshalledEntryUtil.create(k(m), v(m), cache));
      assert loader.contains(k(m));
      assert cache.get(k(m)).equals(v(m));
      assertActivationCount(0);
      assert loader.contains(k(m));
   }

   public void testActivationOnPut(Method m) {
      assertActivationCount(0);
      assert cache.get(k(m)) == null;
      assertActivationCount(0);
      loader.write(MarshalledEntryUtil.create(k(m), v(m), cache));
      assert loader.contains(k(m));
      cache.put(k(m), v(m, 2));
      assert cache.get(k(m)).equals(v(m, 2));
      assertActivationCount(0);
      assert loader.contains(k(m));
   }

   public void testActivationOnReplace(Method m) {
      assertActivationCount(0);
      assertNull(cache.get(k(m)));
      assertActivationCount(0);
      loader.write(MarshalledEntryUtil.create(k(m), v(m), cache));
      assertTrue(loader.contains(k(m)));

      Object prev = cache.replace(k(m), v(m, 2));
      assertNotNull(prev);
      assertEquals(v(m), prev);
      assertActivationCount(0);
      assertTrue(loader.contains(k(m)));
   }

   public void testActivationOnPutMap(Method m) {
      assertActivationCount(0);
      assertNull(cache.get(k(m)));
      assertActivationCount(0);
      loader.write(MarshalledEntryUtil.create(k(m), v(m), cache));
      assertTrue(loader.contains(k(m)));

      Map<String, String> toAdd = new HashMap<>();
      toAdd.put(k(m), v(m, 2));
      cache.putAll(toAdd);
      assertActivationCount(0);
      Object obj = cache.get(k(m));
      assertNotNull(obj);
      assertEquals(v(m, 2), obj);
      assertTrue(loader.contains(k(m)));
   }

   public void testPassivationOnEvict(Method m) throws Exception {
      assertPassivationCount(0);
      cache.put(k(m), v(m));
      cache.put(k(m, 2), v(m, 2));
      cache.evict(k(m));
      assertPassivationCount(1);
      cache.evict(k(m, 2));
      assertPassivationCount(2);
      cache.evict("not_existing_key");
      assertPassivationCount(2);
   }

   public void testPassivateAll(Method m) throws Exception {
      assertPassivationCount(0);
      for (int i = 0; i < 10; i++) {
         cache.put(k(m, i), v(m, i));
      }
      passivateAll();
      assertPassivationCount(9);
   }

   private void assertActivationCount(long activationCount) {
      eventuallyEquals(activationCount, () -> {
         try {
            return Long.parseLong(mBeanServerLookup.getMBeanServer()
                  .getAttribute(activationInterceptorObjName, "Activations").toString());
         } catch (Exception e) {
            throw Util.rewrapAsCacheException(e);
         }
      });
   }

   private void assertPassivationCount(long activationCount) throws Exception {
      long passivations = Long.parseLong(mBeanServerLookup.getMBeanServer()
            .getAttribute(passivationInterceptorObjName, "Passivations").toString());
      assertEquals(activationCount, passivations);
   }
}
