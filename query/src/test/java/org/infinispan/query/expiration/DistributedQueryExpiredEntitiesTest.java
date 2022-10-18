package org.infinispan.query.expiration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;

import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.test.annotation.TestForIssue;
import org.infinispan.commons.time.ControlledTimeService;
import org.infinispan.commons.time.TimeService;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.Search;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.dsl.QueryResult;
import org.infinispan.query.model.Game;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

@TestForIssue(jiraKey = "ISPN-14119")
public class DistributedQueryExpiredEntitiesTest extends MultipleCacheManagersTest {

   private static final String CACHE_NAME = "games";
   private static final int TIME = 100;

   private static final ControlledTimeService timeService = new ControlledTimeService();

   @Override
   protected void createCacheManagers() throws Throwable {
      GlobalConfigurationBuilder global = GlobalConfigurationBuilder.defaultClusteredBuilder();
      ConfigurationBuilder config = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC);

      createClusteredCaches(3, global, config, true, "default");
      EmbeddedCacheManager cacheManager = cacheManagers.get(0); // use the first one

      TestingUtil.replaceComponent(cacheManager, TimeService.class, timeService, true);

      config.encoding().mediaType(MediaType.APPLICATION_PROTOSTREAM_TYPE);
      config.expiration()
            .lifespan(TIME, TimeUnit.MILLISECONDS)
            .maxIdle(TIME, TimeUnit.MILLISECONDS);
      config.indexing().enable()
            .storage(LOCAL_HEAP)
            .addIndexedEntity("org.infinispan.query.model.Game");
      config.statistics().enable();

      cacheManager.createCache(CACHE_NAME, config.build());
   }

   @Test
   public void testQueryExpiredEntities() {
      Cache<Integer, Game> cache = cacheManagers.get(0).getCache(CACHE_NAME);
      cache.put(1, new Game("Ultima IV: Quest of the Avatar", "It is the first in the \"Age of Enlightenment\" trilogy ..."));

      QueryFactory factory = Search.getQueryFactory(cache);
      Query<Game> query = factory.create("from org.infinispan.query.model.Game where description : 'trilogy'");
      QueryResult<Game> result = query.execute();

      assertThat(result.hitCount()).hasValue(1L);
      assertThat(result.list()).extracting("name").contains("Ultima IV: Quest of the Avatar");

      timeService.advance(TIME * 2);

      query = factory.create("from org.infinispan.query.model.Game where description : 'trilogy'");
      result = query.execute();

      assertThat(result.hitCount()).hasValue(1L);

      // verify that the result list does not contain any null value, but it is empty in this case
      assertThat(result.list()).isEmpty();
   }
}
