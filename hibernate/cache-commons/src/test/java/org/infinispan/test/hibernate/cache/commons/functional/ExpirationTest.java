package org.infinispan.test.hibernate.cache.commons.functional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.hibernate.cache.spi.access.AccessType;
import org.hibernate.engine.transaction.jta.platform.internal.NoJtaPlatform;
import org.hibernate.resource.transaction.backend.jdbc.internal.JdbcResourceLocalTransactionCoordinatorBuilderImpl;
import org.infinispan.AdvancedCache;
import org.infinispan.commons.time.ControlledTimeService;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.hibernate.cache.commons.InfinispanBaseRegion;
import org.infinispan.hibernate.cache.commons.util.Caches;
import org.infinispan.hibernate.cache.spi.InfinispanProperties;
import org.infinispan.test.hibernate.cache.commons.functional.entities.Item;
import org.infinispan.test.hibernate.cache.commons.util.TestRegionFactory;
import org.junit.After;
import org.junit.Test;

public class ExpirationTest extends SingleNodeTest {

   private static final long ENTITY_LIFESPAN = 5000;
   private static final ControlledTimeService TIME_SERVICE = new ControlledTimeService();

   @Override
   public List<Object[]> getParameters() {
      return Arrays.asList(
            new Object[]{"read-write", NoJtaPlatform.class, JdbcResourceLocalTransactionCoordinatorBuilderImpl.class, null, AccessType.READ_WRITE, CacheMode.LOCAL, false, false},
            READ_WRITE_INVALIDATION,
            READ_WRITE_REPLICATED,
            READ_WRITE_DISTRIBUTED
      );
   }

   @Override
   @SuppressWarnings("unchecked")
   protected void addSettings(Map settings) {
      super.addSettings(settings);
      settings.put(TestRegionFactory.TIME_SERVICE, TIME_SERVICE);
      settings.put(InfinispanProperties.PREFIX + InfinispanProperties.ENTITY + InfinispanProperties.LIFESPAN_SUFFIX,
            String.valueOf(ENTITY_LIFESPAN));
      // Override default max-idle (100000) so it doesn't conflict with lifespan
      settings.put(InfinispanProperties.PREFIX + InfinispanProperties.ENTITY + InfinispanProperties.MAX_IDLE_SUFFIX,
            "-1");
      if (cacheMode == CacheMode.LOCAL) {
         settings.put(InfinispanProperties.INFINISPAN_CONFIG_RESOURCE_PROP, InfinispanProperties.INFINISPAN_CONFIG_LOCAL_RESOURCE);
      }
   }

   @After
   public void cleanup() throws Exception {
      withTxSession(s -> TEST_SESSION_ACCESS.execQueryUpdate(s, "delete from Item"));
   }

   @Test
   public void testEntityExpiresAfterLifespan() throws Exception {
      Item item = new Item("expiring", "Expiring item");
      withTxSession(s -> s.persist(item));

      // Load entity in new session to ensure L2 cache is populated
      withTxSession(s -> {
         Item loaded = s.get(Item.class, item.getId());
         assertNotNull(loaded);
         assertEquals("Expiring item", loaded.getDescription());
      });

      // Verify entity is served from L2 cache
      sessionFactory().getStatistics().clear();
      withTxSession(s -> {
         Item loaded = s.get(Item.class, item.getId());
         assertNotNull(loaded);
         assertEquals("Expiring item", loaded.getDescription());
      });
      assertEquals("Expected L2 cache hit", 1,
            sessionFactory().getStatistics().getSecondLevelCacheHitCount());

      TIME_SERVICE.advance(ENTITY_LIFESPAN + 1);

      // Entity should have expired from L2 cache
      sessionFactory().getStatistics().clear();
      withTxSession(s -> {
         Item loaded = s.get(Item.class, item.getId());
         assertNotNull(loaded);
         assertEquals("Expiring item", loaded.getDescription());
      });
      assertEquals("Expected L2 cache miss after expiration", 0,
            sessionFactory().getStatistics().getSecondLevelCacheHitCount());
      assertTrue("Expected at least one L2 cache miss",
            sessionFactory().getStatistics().getSecondLevelCacheMissCount() > 0);
   }

   @Test
   public void testEntityCacheEntryExpiresWithLifespan() throws Exception {
      Item item = new Item("expiring", "Expiring item");
      withTxSession(s -> s.persist(item));

      InfinispanBaseRegion region = TEST_SESSION_ACCESS.getRegion(sessionFactory(), Item.class.getName());
      AdvancedCache entityCache = region.getCache();

      // Load to ensure cache is populated
      withTxSession(s -> {
         Item loaded = s.get(Item.class, item.getId());
         assertNotNull(loaded);
      });

      // Verify cache has entries
      Map contents = Caches.entrySet(entityCache).toMap();
      assertTrue("Cache should not be empty", contents.size() > 0);

      TIME_SERVICE.advance(ENTITY_LIFESPAN + 1);

      // Trigger lazy expiration and verify cache is empty
      for (Object key : contents.keySet()) {
         entityCache.get(key);
      }

      Map afterExpiration = Caches.entrySet(entityCache).toMap();
      assertEquals("Cache should be empty after expiration", 0, afterExpiration.size());
   }
}
