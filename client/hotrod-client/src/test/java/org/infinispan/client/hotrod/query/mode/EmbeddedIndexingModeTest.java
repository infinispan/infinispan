package org.infinispan.client.hotrod.query.mode;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;

import org.infinispan.Cache;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.commons.api.query.Query;
import org.infinispan.commons.api.query.QueryResult;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.IndexingMode;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.query.Search;
import org.infinispan.query.core.stats.QueryStatistics;
import org.infinispan.query.model.Game;
import org.infinispan.server.core.admin.embeddedserver.EmbeddedServerAdminOperationHandler;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "client.hotrod.query.mode.EmbeddedIndexingModeTest")
public class EmbeddedIndexingModeTest extends SingleHotRodServerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() {
      ConfigurationBuilder xJavaObject = new ConfigurationBuilder();
      xJavaObject
            .statistics().enable()
            .encoding()
               // if the cache is object encoded (not protobuf encoded)
               .mediaType(MediaType.APPLICATION_OBJECT_TYPE)
            .indexing()
               .enable()
               .storage(LOCAL_HEAP)
               // the indexes will be initialized as embedded indexes
               .addIndexedEntity(Game.class.getName());

      ConfigurationBuilder xProtoStream = new ConfigurationBuilder();
      xProtoStream
            .statistics().enable()
            .encoding()
               // even if the cache is protobuf encoded
               .mediaType(MediaType.APPLICATION_PROTOSTREAM)
            .indexing()
               .enable()
               // if I force the indexes to be defined on Java classes
               .useJavaEmbeddedEntities()
               .storage(LOCAL_HEAP)
               // the indexes will be initialized as embedded indexes
               .addIndexedEntity(Game.class.getName());

      ConfigurationBuilder xProtoStreamManual = new ConfigurationBuilder();
      xProtoStreamManual
            .statistics().enable()
            .encoding()
               // even if the cache is protobuf encoded
               .mediaType(MediaType.APPLICATION_PROTOSTREAM)
            .indexing()
               .enable()
               // if I force the indexes to be defined on Java classes
               .useJavaEmbeddedEntities()
               .storage(LOCAL_HEAP)
               // the indexes will be initialized as embedded indexes (so far as before!)
               .addIndexedEntity(Game.class.getName())
               // but now I want to test the reindexing
               .indexingMode(IndexingMode.MANUAL);

      EmbeddedCacheManager manager = TestCacheManagerFactory.createServerModeCacheManager();
      manager.defineConfiguration("x-java-object", xJavaObject.build());
      manager.defineConfiguration("x-protostream", xProtoStream.build());
      manager.defineConfiguration("x-protostream-manual", xProtoStreamManual.build());
      return manager;
   }

   @Override
   protected SerializationContextInitializer contextInitializer() {
      return Game.GameSchema.INSTANCE;
   }

   @Override
   protected HotRodServer createHotRodServer() {
      HotRodServerConfigurationBuilder serverBuilder = new HotRodServerConfigurationBuilder();
      serverBuilder.adminOperationsHandler(new EmbeddedServerAdminOperationHandler());
      return HotRodClientTestingUtil.startHotRodServer(cacheManager, serverBuilder);
   }

   @Test
   public void smoke_xJavaObject() {
      RemoteCache<Integer, Game> remoteCache = remoteCacheManager.getCache("x-java-object");
      Cache<Object, Object> embCache = cacheManager.getCache("x-java-object");
      autoIndexing(embCache, remoteCache);
   }

   @Test
   public void smoke_xProtoStream() {
      RemoteCache<Integer, Game> remoteCache = remoteCacheManager.getCache("x-protostream");
      Cache<Object, Object> embCache = cacheManager.getCache("x-protostream");
      autoIndexing(embCache, remoteCache);
   }

   @Test
   public void smoke_xProtoStream_reindexing() {
      RemoteCache<Integer, Game> remoteCache = remoteCacheManager.getCache("x-protostream-manual");
      Cache<Object, Object> embCache = cacheManager.getCache("x-protostream-manual");
      manualIndexing(embCache, remoteCache);
   }

   private static void autoIndexing(Cache<Object, Object> embCache, RemoteCache<Integer, Game> remoteCache) {
      QueryStatistics queryStatistics = Search.getSearchStatistics(embCache).getQueryStatistics();
      queryStatistics.clear();

      // insert an entity using the embedded cache api
      embCache.put(1, new Game("Civilization 1", "The best video game of all time!")); // according to the contributor

      // insert another entity using the remote cache api
      remoteCache.put(2, new Game("Ultima IV: Quest of the Avatar", "It is the first in the \"Age of Enlightenment\" trilogy ..."));

      Query<Game> query = embCache.query("from org.infinispan.query.model.Game where description : 'the' order by name");
      QueryResult<Game> result = query.execute();

      assertThat(result.count().exact()).isTrue();
      assertThat(result.count().value()).isEqualTo(2);
      assertThat(result.list()).extracting("name").containsExactlyInAnyOrder("Civilization 1", "Ultima IV: Quest of the Avatar");
      assertThat(queryStatistics.getLocalIndexedQueryCount()).isOne();

      // insert a third entity using the result get from the query
      remoteCache.put(3, result.list().get(0));

      query = embCache.query("from org.infinispan.query.model.Game where description : 'the' order by name");
      result = query.execute();

      assertThat(result.count().value()).isEqualTo(3);
      assertThat(result.list()).extracting("name").containsExactlyInAnyOrder("Civilization 1", "Ultima IV: Quest of the Avatar", "Civilization 1");
      assertThat(queryStatistics.getLocalIndexedQueryCount()).isEqualTo(2);
   }

   private void manualIndexing(Cache<Object, Object> embCache, RemoteCache<Integer, Game> remoteCache) {
      QueryStatistics queryStatistics = Search.getSearchStatistics(embCache).getQueryStatistics();
      queryStatistics.clear();

      // insert an entity using the embedded cache api
      embCache.put(1, new Game("Civilization 1", "The best video game of all time!")); // according to the contributor

      // insert another entity using the remote cache api
      remoteCache.put(2, new Game("Ultima IV: Quest of the Avatar", "It is the first in the \"Age of Enlightenment\" trilogy ..."));

      Query<Game> query = embCache.query("from org.infinispan.query.model.Game where description : 'the' order by name");
      QueryResult<Game> result = query.execute();

      assertThat(result.count().value()).isZero();

      remoteCacheManager.administration().reindexCache(embCache.getName());
      result = query.execute();

      assertThat(result.count().exact()).isTrue();
      assertThat(result.count().value()).isEqualTo(2);
      assertThat(result.list()).extracting("name").containsExactlyInAnyOrder("Civilization 1", "Ultima IV: Quest of the Avatar");
      assertThat(queryStatistics.getLocalIndexedQueryCount()).isEqualTo(2);

      // insert a third entity using the result get from the query
      remoteCache.put(3, result.list().get(0));

      query = embCache.query("from org.infinispan.query.model.Game where description : 'the' order by name");
      result = query.execute();

      assertThat(result.count().value()).isEqualTo(2);

      remoteCacheManager.administration().reindexCache(embCache.getName());
      result = query.execute();

      assertThat(result.count().value()).isEqualTo(3);
      assertThat(result.list()).extracting("name").containsExactlyInAnyOrder("Civilization 1", "Ultima IV: Quest of the Avatar", "Civilization 1");
      assertThat(queryStatistics.getLocalIndexedQueryCount()).isEqualTo(4);
   }
}
