package org.infinispan.test.hibernate.cache.commons.functional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.time.ControlledTimeService;
import org.infinispan.hibernate.cache.commons.InfinispanBaseRegion;
import org.infinispan.hibernate.cache.commons.util.Caches;
import org.infinispan.hibernate.cache.spi.InfinispanProperties;
import org.infinispan.test.hibernate.cache.commons.functional.entities.Item;
import org.infinispan.test.hibernate.cache.commons.util.TestRegionFactory;
import org.junit.After;
import org.junit.Test;

public class MaxIdleExpirationTest extends SingleNodeTest {

   private static final long ENTITY_MAX_IDLE = 5000;
   private static final ControlledTimeService TIME_SERVICE = new ControlledTimeService();

   @Override
   public List<Object[]> getParameters() {
      return Arrays.asList(
            READ_WRITE_REPLICATED,
            READ_WRITE_DISTRIBUTED
      );
   }

   @Override
   @SuppressWarnings("unchecked")
   protected void addSettings(Map settings) {
      super.addSettings(settings);
      settings.put(TestRegionFactory.TIME_SERVICE, TIME_SERVICE);
      settings.put(InfinispanProperties.PREFIX + InfinispanProperties.ENTITY + InfinispanProperties.MAX_IDLE_SUFFIX,
            String.valueOf(ENTITY_MAX_IDLE));
   }

   @After
   public void cleanup() throws Exception {
      withTxSession(s -> TEST_SESSION_ACCESS.execQueryUpdate(s, "delete from Item"));
   }

   @Test
   public void testEntityExpiresAfterMaxIdle() throws Exception {
      Item item = new Item("expiring", "Expiring item");
      withTxSession(s -> s.persist(item));

      withTxSession(s -> {
         Item loaded = s.get(Item.class, item.getId());
         assertNotNull(loaded);
         assertEquals("Expiring item", loaded.getDescription());
      });

      sessionFactory().getStatistics().clear();
      withTxSession(s -> {
         Item loaded = s.get(Item.class, item.getId());
         assertNotNull(loaded);
         assertEquals("Expiring item", loaded.getDescription());
      });
      assertEquals(1, sessionFactory().getStatistics().getSecondLevelCacheHitCount(), "Expected L2 cache hit");

      TIME_SERVICE.advance(ENTITY_MAX_IDLE + 1);

      sessionFactory().getStatistics().clear();
      withTxSession(s -> {
         Item loaded = s.get(Item.class, item.getId());
         assertNotNull(loaded);
         assertEquals("Expiring item", loaded.getDescription());
      });
      assertEquals(0,
            sessionFactory().getStatistics().getSecondLevelCacheHitCount(), "Expected L2 cache miss after max-idle expiration");
      assertTrue(sessionFactory().getStatistics().getSecondLevelCacheMissCount() > 0, "Expected at least one L2 cache miss");
   }

   @Test
   public void testEntityCacheEntryExpiresWithMaxIdle() throws Exception {
      Item item = new Item("expiring", "Expiring item");
      withTxSession(s -> s.persist(item));

      InfinispanBaseRegion region = TEST_SESSION_ACCESS.getRegion(sessionFactory(), Item.class.getName());
      AdvancedCache entityCache = region.getCache();

      withTxSession(s -> {
         Item loaded = s.get(Item.class, item.getId());
         assertNotNull(loaded);
      });

      Map contents = Caches.entrySet(entityCache).toMap();
      assertTrue(contents.size() > 0, "Cache should not be empty");

      TIME_SERVICE.advance(ENTITY_MAX_IDLE + 1);

      for (Object key : contents.keySet()) {
         entityCache.get(key);
      }

      Map afterExpiration = Caches.entrySet(entityCache).toMap();
      assertEquals(0, afterExpiration.size(), "Cache should be empty after max-idle expiration");
   }
}
