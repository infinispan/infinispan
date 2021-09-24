package org.infinispan.expiration.impl;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;

import java.util.concurrent.TimeUnit;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "expiration.impl.ExpirationStoreFunctionalTest")
public class ExpirationStoreFunctionalTest extends ExpirationFunctionalTest {

   private boolean passivationEnabled;

   private final int MAX_IN_MEMORY = 5;

   ExpirationStoreFunctionalTest passivation(boolean enable) {
      this.passivationEnabled = enable;
      return this;
   }

   @Factory
   @Override
   public Object[] factory() {
      return new Object[]{
            // Test is for dummy store and we don't care about memory storage types
            new ExpirationStoreFunctionalTest().passivation(true).cacheMode(CacheMode.LOCAL),
            new ExpirationStoreFunctionalTest().passivation(false).cacheMode(CacheMode.LOCAL),
      };
   }

   @Override
   protected String parameters() {
      return "[passivation= " + passivationEnabled + "]";
   }

   @Override
   protected void configure(ConfigurationBuilder config) {
      config
            .memory().maxCount(passivationEnabled ? MAX_IN_MEMORY : -1)
            // Prevent the reaper from running, reaperEnabled(false) doesn't work when a store is present
            .expiration().wakeUpInterval(Long.MAX_VALUE)
            .persistence().passivation(passivationEnabled)
            .addStore(DummyInMemoryStoreConfigurationBuilder.class);
   }

   @Override
   protected int maxInMemory() {
      return passivationEnabled ? MAX_IN_MEMORY : super.maxInMemory();
   }

   public void testMaxIdleWithPassivation() {
      cache.put("will-expire", "uh oh", -1, null, 1, TimeUnit.MILLISECONDS);
      // Approximately half of these will be in memory with passivation, with rest in the store
      for (int i = 0; i < SIZE; i++) {
         cache.put("key-" + i, "value-" + i, -1, null, 10, TimeUnit.MILLISECONDS);
      }

      assertEquals(SIZE + 1, cache.size());

      timeService.advance(6);

      assertEquals(SIZE, cache.size());

      // Now we read just a few of them
      assertNotNull(cache.get("key-" + 1));
      assertNotNull(cache.get("key-" + 6));
      assertNotNull(cache.get("key-" + 3));

      processExpiration();

      // This will expire all but the 3 we touched above
      timeService.advance(6);

      assertEquals(3, cache.size());

      // This will expire the rest
      timeService.advance(6);

      assertEquals(0, cache.size());

      processExpiration();

      assertEquals(0, cache.getAdvancedCache().getDataContainer().sizeIncludingExpired());
   }
}
