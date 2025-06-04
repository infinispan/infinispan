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

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.infinispan.commons.jmx.MBeanServerLookup;
import org.infinispan.commons.jmx.TestMBeanServerLookup;
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
 * Tester class for PassivationInterceptor.
 *
 * @author Mircea.Markus@jboss.com
 * @author Galder Zamarreño
 */
@Test(groups = "functional", testName = "jmx.PassivationInterceptorMBeanTest")
public class PassivationInterceptorMBeanTest extends SingleCacheManagerTest {

   private static final String JMX_DOMAIN = PassivationInterceptorMBeanTest.class.getSimpleName();

   private final MBeanServerLookup mBeanServerLookup = TestMBeanServerLookup.create();

   private DummyInMemoryStore<Object, Object> loader;
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
      passivationInterceptorObjName =
            getCacheObjectName(JMX_DOMAIN, getDefaultCacheName() + "(local)", "Passivation");
      loader = TestingUtil.getFirstStore(cache);
   }

   @AfterMethod
   public void resetStats() throws Exception {
      MBeanServer mBeanServer = mBeanServerLookup.getMBeanServer();
      mBeanServer.invoke(passivationInterceptorObjName, "resetStatistics", new Object[0], new String[0]);
   }

   private void passivateAll() throws Exception {
      mBeanServerLookup.getMBeanServer().invoke(passivationInterceptorObjName, "passivateAll", new Object[0], new String[0]);
   }

   public void testActivationOnPut(Method m) {
      assertNull(cache.get(k(m)));
      loader.write(MarshalledEntryUtil.create(k(m), v(m), cache));
      assertTrue(loader.contains(k(m)));
      cache.put(k(m), v(m, 2));
      assertEquals(v(m, 2), cache.get(k(m)));
      assertTrue(loader.contains(k(m)));
   }

   public void testActivationOnReplace(Method m) {
      assertNull(cache.get(k(m)));
      loader.write(MarshalledEntryUtil.create(k(m), v(m), cache));
      assertTrue(loader.contains(k(m)));

      Object prev = cache.replace(k(m), v(m, 2));
      assertNotNull(prev);
      assertEquals(v(m), prev);
      assertTrue(loader.contains(k(m)));
   }

   public void testActivationOnPutMap(Method m) {
      assertNull(cache.get(k(m)));
      loader.write(MarshalledEntryUtil.create(k(m), v(m), cache));
      assertTrue(loader.contains(k(m)));

      Map<String, String> toAdd = new HashMap<>();
      toAdd.put(k(m), v(m, 2));
      cache.putAll(toAdd);
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

   private void assertPassivationCount(long activationCount) throws Exception {
      long passivations = Long.parseLong(mBeanServerLookup.getMBeanServer()
            .getAttribute(passivationInterceptorObjName, "Passivations").toString());
      assertEquals(activationCount, passivations);
   }
}
