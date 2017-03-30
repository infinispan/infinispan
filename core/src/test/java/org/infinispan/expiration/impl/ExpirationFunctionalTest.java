package org.infinispan.expiration.impl;

import static org.testng.AssertJUnit.assertEquals;

import java.util.concurrent.TimeUnit;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.ControlledTimeService;
import org.infinispan.util.TimeService;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "expiration.impl.ExpirationFunctionalTest")
public class ExpirationFunctionalTest extends SingleCacheManagerTest {

   protected final int SIZE = 10;
   protected ControlledTimeService timeService = new ControlledTimeService();

   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder builder = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      configure(builder);
      EmbeddedCacheManager cm = createCacheManager(builder);
      TestingUtil.replaceComponent(cm, TimeService.class, timeService, true);
      cache = cm.getCache();
      afterCacheCreated(cm);
      return cm;
   }

   protected EmbeddedCacheManager createCacheManager(ConfigurationBuilder builder) {
      return TestCacheManagerFactory.createCacheManager(builder);
   }

   protected void configure(ConfigurationBuilder config) {
      config.expiration().disableReaper();
   }

   protected void afterCacheCreated(EmbeddedCacheManager cm) {

   }

   public void testSimpleExpirationLifespan() throws Exception {

      for (int i = 0; i < SIZE; i++) {
         cache.put("key-" + i, "value-" + i, 1, TimeUnit.MILLISECONDS);
      }
      timeService.advance(2);
      assertEquals(0, cache.size());
   }

   public void testSimpleExpirationMaxIdle() throws Exception {

      for (int i = 0; i < SIZE; i++) {
         cache.put("key-" + i, "value-" + i,-1, null, 1, TimeUnit.MILLISECONDS);
      }
      timeService.advance(2);
      assertEquals(0, cache.size());
   }
}
