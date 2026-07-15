package org.infinispan.query.core.tests;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;

import java.util.List;

import org.infinispan.Cache;
import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.commons.api.query.Query;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.model.Game;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "query.core.tests.UpdateQueryCoreTest")
public class UpdateQueryCoreTest extends SingleCacheManagerTest {

   private static final String ENTITY = "org.infinispan.query.model.Game";

   private Cache<String, Game> gameCache;

   @Override
   protected EmbeddedCacheManager createCacheManager() {
      ConfigurationBuilder builder = getDefaultStandaloneCacheConfig(false);
      builder.encoding().mediaType(MediaType.APPLICATION_PROTOSTREAM_TYPE)
            .indexing().enable().storage(LOCAL_HEAP)
            .addIndexedEntity(ENTITY);

      cacheManager = TestCacheManagerFactory.createCacheManager(Game.GameSchema.INSTANCE, null);
      gameCache = cacheManager.administration()
            .withFlags(CacheContainerAdmin.AdminFlag.VOLATILE)
            .getOrCreateCache("update-test", builder.build());
      return cacheManager;
   }

   @BeforeMethod
   public void populateCache() {
      gameCache.clear();
      gameCache.put("g1", new Game("Civilization", "The best strategy game"));
      gameCache.put("g2", new Game("Doom", "First person shooter classic"));
      gameCache.put("g3", new Game("Tetris", "Puzzle game with blocks"));
   }

   @Test
   public void testUpdateSetStringField() {
      Query<Game> update = gameCache.query(
            "update from " + ENTITY + " set name = 'Civilization VI' where name = 'Civilization'");
      int count = update.executeStatement();

      assertThat(count).isEqualTo(1);

      Game result = gameCache.get("g1");
      assertThat(result.getName()).isEqualTo("Civilization VI");
      assertThat(result.getDescription()).isEqualTo("The best strategy game");
   }

   @Test
   public void testUpdateMultipleFields() {
      Query<Game> update = gameCache.query(
            "update from " + ENTITY + " set name = 'DOOM Eternal', set description = 'Rip and tear' where name = 'Doom'");
      int count = update.executeStatement();

      assertThat(count).isEqualTo(1);

      Game result = gameCache.get("g2");
      assertThat(result.getName()).isEqualTo("DOOM Eternal");
      assertThat(result.getDescription()).isEqualTo("Rip and tear");
   }

   @Test
   public void testUpdateDoesNotAffectNonMatching() {
      Query<Game> update = gameCache.query(
            "update from " + ENTITY + " set name = 'changed' where name = 'nonexistent'");
      int count = update.executeStatement();

      assertThat(count).isEqualTo(0);

      assertThat(gameCache.get("g1").getName()).isEqualTo("Civilization");
      assertThat(gameCache.get("g2").getName()).isEqualTo("Doom");
      assertThat(gameCache.get("g3").getName()).isEqualTo("Tetris");
   }

   @Test
   public void testUpdateSetFieldToNull() {
      Query<Game> update = gameCache.query(
            "update from " + ENTITY + " set description = null where name = 'Tetris'");
      int count = update.executeStatement();

      assertThat(count).isEqualTo(1);

      Game result = gameCache.get("g3");
      assertThat(result.getDescription()).isNull();
      assertThat(result.getName()).isEqualTo("Tetris");
   }

   @Test
   public void testSelectStillWorksAfterUpdate() {
      gameCache.query("update from " + ENTITY + " set name = 'Updated' where name = 'Civilization'")
            .executeStatement();

      Query<Game> select = gameCache.query("from " + ENTITY + " where name = 'Updated'");
      List<Game> results = select.execute().list();

      assertThat(results).hasSize(1);
      assertThat(results.get(0).getDescription()).isEqualTo("The best strategy game");
   }

   // TODO: index re-sync after update via storageCache.put() - needs QueryInterceptor integration
   @Test
   public void testCacheGetAfterUpdate() {
      gameCache.query("update from " + ENTITY + " set name = 'Civ VI' where name = 'Civilization'")
            .executeStatement();

      Game result = gameCache.get("g1");
      assertThat(result.getName()).isEqualTo("Civ VI");
      assertThat(result.getDescription()).isEqualTo("The best strategy game");
      assertThat(gameCache.size()).isEqualTo(3);
   }
}
