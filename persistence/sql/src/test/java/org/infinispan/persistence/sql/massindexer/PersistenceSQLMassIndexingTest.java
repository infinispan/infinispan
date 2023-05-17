package org.infinispan.persistence.sql.massindexer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.util.concurrent.CompletionStages.join;
import static org.testng.Assert.assertEquals;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.infinispan.Cache;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.IndexStorage;
import org.infinispan.configuration.cache.IndexingMode;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.jdbc.common.DatabaseType;
import org.infinispan.persistence.jdbc.common.SqlManager;
import org.infinispan.persistence.jdbc.common.configuration.ConnectionFactoryConfiguration;
import org.infinispan.persistence.jdbc.common.configuration.ConnectionFactoryConfigurationBuilder;
import org.infinispan.persistence.jdbc.common.connectionfactory.ConnectionFactory;
import org.infinispan.persistence.sql.configuration.QueriesJdbcStoreConfigurationBuilder;
import org.infinispan.query.Indexer;
import org.infinispan.query.Search;
import org.infinispan.query.core.stats.IndexInfo;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.CleanupAfterTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "persistence.sql.massindexer.PersistenceSQLMassIndexingTest")
@CleanupAfterTest
public class PersistenceSQLMassIndexingTest extends SingleCacheManagerTest {

   private static final String ANOTHER_CACHE = "anotherCache";

   private static final String DESCRIPTION = "Lorem ipsum dolor sit amet, consectetur adipiscing elit, " +
         "sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Cum sociis natoque penatibus et magnis." +
         " Vel pharetra vel turpis nunc eget. Ullamcorper eget nulla facilisi etiam dignissim diam. " +
         "Bibendum arcu vitae elementum curabitur vitae nunc sed.";

   private static final int NUM_ENTITIES = 10_000;

   @Override
   protected EmbeddedCacheManager createCacheManager() {
      ConfigurationBuilder config = new ConfigurationBuilder();
      QueriesJdbcStoreConfigurationBuilder jdbcStoreConfig = config
            .clustering()
            .cacheMode(CacheMode.REPL_SYNC)
            .encoding()
               .mediaType(MediaType.APPLICATION_PROTOSTREAM_TYPE)
            .indexing()
               .enable()
               .storage(IndexStorage.LOCAL_HEAP)
               .indexingMode(IndexingMode.MANUAL)
               .addIndexedEntity(GamePlayer.class)
            .persistence()
               .passivation(false)
               .addStore(QueriesJdbcStoreConfigurationBuilder.class)
                  .preload(false);

      SqlManager sqlManager = SqlManager.fromDatabaseType(DatabaseType.H2, "GamePlayer", true);

      List<String> keyColumns = Collections.singletonList("id");
      List<String> allColumns = Arrays.asList("id", "nick", "ranking", "game", "history");

      jdbcStoreConfig.queries()
            .size("select count(*) from GamePlayer")
            .select("select id, nick, ranking, game, history from GamePlayer where id = :id")
            .selectAll("select id, nick, ranking, game, history from GamePlayer")
            .upsert(sqlManager.getUpsertStatement(keyColumns, allColumns))
            .delete("delete from GamePlayer where id = :id")
            .deleteAll("delete from GamePlayer");

      jdbcStoreConfig
            .connectionPool()
            .driverClass(org.h2.Driver.class)
            .connectionUrl("jdbc:h2:mem:players;DB_CLOSE_DELAY=-1")
            .username("sa");

      jdbcStoreConfig.keyColumns("id");
      jdbcStoreConfig.schema()
            .packageName("play")
            .messageName("GamePlayer")
            .embeddedKey(false);

      createTable(jdbcStoreConfig);

      GlobalConfigurationBuilder globalBuilder = new GlobalConfigurationBuilder().clusteredDefault();
      globalBuilder.serialization().addContextInitializer(GamePlayer.GamePlayerSchema.INSTANCE);

      EmbeddedCacheManager manager = TestCacheManagerFactory.createClusteredCacheManager(globalBuilder, config);
      manager.defineConfiguration(ANOTHER_CACHE, config.build());

      return manager;
   }

   @Override
   protected void setup() throws Exception {
      super.setup();
      populateTheCache();
   }

   @Test
   public void tryLoop() {
      AtomicInteger sink = new AtomicInteger();

      long start = System.currentTimeMillis();
      try (Stream<CacheEntry<Object, Object>> stream = cache.getAdvancedCache().withFlags(Flag.CACHE_MODE_LOCAL)
            .cacheEntrySet().stream()) {
         stream.forEach(
               (val) -> {
                  GamePlayer player = (GamePlayer) val.getValue();
                  sink.addAndGet(player.getRanking());
               }
         );
      }
      long duration = System.currentTimeMillis() - start;
      log.infov("try-loop duration -> " + duration);

      assertEquals(sink.get(), 45_000);
   }

   @Test
   public void testMassIndexer() {
      populateTheCache();

      IndexInfo indexInfo = getIndexInfo(cache);
      assertThat(indexInfo.count()).isZero();

      Indexer massIndexer = Search.getIndexer(cache);
      runIndexer(massIndexer);

      indexInfo = getIndexInfo(cache);
      assertThat(indexInfo.count()).isEqualTo(NUM_ENTITIES);

      // reindex another cache using the same cache store
      Cache<Integer, GamePlayer> anotherCache = cacheManager.getCache(ANOTHER_CACHE);

      indexInfo = getIndexInfo(anotherCache);
      assertThat(indexInfo.count()).isZero();

      massIndexer = Search.getIndexer(anotherCache);
      runIndexer(massIndexer);

      indexInfo = getIndexInfo(anotherCache);
      assertThat(indexInfo.count()).isEqualTo(NUM_ENTITIES);
   }

   private void populateTheCache() {
      HashMap<Integer, GamePlayer> players = new HashMap<>();
      for (int k = 0; k < NUM_ENTITIES; k++) {
         players.put(k, new GamePlayer("nick-" + k, k % 10, "Dark Age Of Camelot", DESCRIPTION));
      }
      cache.putAll(players);
   }

   private void createTable(QueriesJdbcStoreConfigurationBuilder jdbcStoreConfig) {
      ConnectionFactoryConfigurationBuilder<ConnectionFactoryConfiguration> connectionFactory =
            jdbcStoreConfig.getConnectionFactory();
      String createTable = "create table GamePlayer (" +
            "id int not null, " +
            "nick varchar(63) not null, " +
            "ranking int, " +
            "game varchar(63), " +
            "history varchar(511)," +
            "primary key (id))";

      ConnectionFactoryConfiguration cfc = connectionFactory.create();
      ConnectionFactory factory = ConnectionFactory.getConnectionFactory(cfc.connectionFactoryClass());
      factory.start(cfc, getClass().getClassLoader());

      Connection connection = null;
      try {
         connection = factory.getConnection();
         try (Statement stmt = connection.createStatement()) {
            stmt.execute(createTable);
         }
      } catch (SQLException t) {
         throw new AssertionError(t);
      } finally {
         factory.releaseConnection(connection);
         factory.stop();
      }
   }

   private IndexInfo getIndexInfo(Cache<?, ?> cache) {
      return join(Search.getSearchStatistics(cache).getIndexStatistics().computeIndexInfos()).get(GamePlayer.class.getName());
   }

   private static void runIndexer(Indexer massIndexer) {
      long start = System.currentTimeMillis();
      join(massIndexer.runLocal());
      long end = System.currentTimeMillis();
      long duration = end - start;
      log.infov("runIndexer duration -> " + duration);
   }
}
