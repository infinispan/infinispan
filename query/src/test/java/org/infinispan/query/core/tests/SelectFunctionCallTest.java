package org.infinispan.query.core.tests;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.infinispan.Cache;
import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.commons.api.query.Query;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.query.BaseQuery;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.protostream.sampledomain.Game;
import org.infinispan.query.impl.QueryEngine;
import org.infinispan.query.objectfilter.ParsingException;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "query.core.tests.SelectFunctionCallTest")
public class SelectFunctionCallTest extends SingleCacheManagerTest {

   private Cache<String, Game> indexedCache;
   private Cache<String, Game> nonIndexedCache;

   @Override
   protected EmbeddedCacheManager createCacheManager() {
      ConfigurationBuilder indexedCfg = getDefaultStandaloneCacheConfig(false);
      indexedCfg.encoding().mediaType(MediaType.APPLICATION_PROTOSTREAM_TYPE)
            .indexing().enable().storage(LOCAL_HEAP)
            .addIndexedEntity(Game.ENTITY_NAME);

      ConfigurationBuilder nonIndexedCfg = getDefaultStandaloneCacheConfig(false);
      nonIndexedCfg.encoding().mediaType(MediaType.APPLICATION_PROTOSTREAM_TYPE);

      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(Game.GameSchema.INSTANCE, null);
      indexedCache = cm.administration()
            .withFlags(CacheContainerAdmin.AdminFlag.VOLATILE)
            .getOrCreateCache("indexed", indexedCfg.build());
      nonIndexedCache = cm.administration()
            .withFlags(CacheContainerAdmin.AdminFlag.VOLATILE)
            .getOrCreateCache("nonIndexed", nonIndexedCfg.build());
      return cm;
   }

   private void populate(Cache<String, Game> cache) {
      cache.clear();
      cache.put("g1", new Game("Civilization", "The best strategy game", 90, 59.99,
            List.of("strategy", "turn-based"), null));
      cache.put("g2", new Game("Doom", "First person shooter classic", 85, 39.99,
            List.of("shooter", "action"), null));
      cache.put("g3", new Game("Tetris", "Puzzle game with blocks", 95, 19.99,
            List.of("puzzle", "arcade"), null));
   }

   @BeforeMethod
   public void setUp() {
      populate(indexedCache);
      populate(nonIndexedCache);
   }

   // --- Indexed path tests ---

   @Test
   public void testConcatIndexed() {
      Query<Object> query = indexedCache.query(
            "select concat(name, ' - ', description) from " + Game.ENTITY_NAME + " order by name");
      List<Object> results = query.list();
      assertThat(results).hasSize(3);
      assertThat(((Object[]) results.get(0))[0]).isEqualTo("Civilization - The best strategy game");
      assertThat(((Object[]) results.get(1))[0]).isEqualTo("Doom - First person shooter classic");
      assertThat(((Object[]) results.get(2))[0]).isEqualTo("Tetris - Puzzle game with blocks");
   }

   @Test
   public void testUpperIndexed() {
      Query<Object> query = indexedCache.query(
            "select upper(name) from " + Game.ENTITY_NAME + " order by name");
      List<Object> results = query.list();
      assertThat(results).hasSize(3);
      assertThat(((Object[]) results.get(0))[0]).isEqualTo("CIVILIZATION");
      assertThat(((Object[]) results.get(1))[0]).isEqualTo("DOOM");
      assertThat(((Object[]) results.get(2))[0]).isEqualTo("TETRIS");
   }

   @Test
   public void testLowerIndexed() {
      Query<Object> query = indexedCache.query(
            "select lower(name) from " + Game.ENTITY_NAME + " order by name");
      List<Object> results = query.list();
      assertThat(results).hasSize(3);
      assertThat(((Object[]) results.get(0))[0]).isEqualTo("civilization");
      assertThat(((Object[]) results.get(1))[0]).isEqualTo("doom");
      assertThat(((Object[]) results.get(2))[0]).isEqualTo("tetris");
   }

   @Test
   public void testNestedFunctionIndexed() {
      Query<Object> query = indexedCache.query(
            "select upper(concat(name, '!')) from " + Game.ENTITY_NAME + " order by name");
      List<Object> results = query.list();
      assertThat(results).hasSize(3);
      assertThat(((Object[]) results.get(0))[0]).isEqualTo("CIVILIZATION!");
      assertThat(((Object[]) results.get(1))[0]).isEqualTo("DOOM!");
      assertThat(((Object[]) results.get(2))[0]).isEqualTo("TETRIS!");
   }

   @Test
   public void testFunctionWithLiteralOnlyIndexed() {
      Query<Object> query = indexedCache.query(
            "select upper('hello') from " + Game.ENTITY_NAME + " where name = 'Doom'");
      List<Object> results = query.list();
      assertThat(results).hasSize(1);
      assertThat(((Object[]) results.get(0))[0]).isEqualTo("HELLO");
   }

   @Test
   public void testSubstringIndexed() {
      Query<Object> query = indexedCache.query(
            "select substring(name, 1, 4) from " + Game.ENTITY_NAME + " order by name");
      List<Object> results = query.list();
      assertThat(results).hasSize(3);
      assertThat(((Object[]) results.get(0))[0]).isEqualTo("Civ");
      assertThat(((Object[]) results.get(1))[0]).isEqualTo("Doo");
      assertThat(((Object[]) results.get(2))[0]).isEqualTo("Tet");
   }

   @Test
   public void testWithWhereClauseIndexed() {
      Query<Object> query = indexedCache.query(
            "select concat(name, '!') from " + Game.ENTITY_NAME + " where name = 'Doom'");
      List<Object> results = query.list();
      assertThat(results).hasSize(1);
      assertThat(((Object[]) results.get(0))[0]).isEqualTo("Doom!");
   }

   @Test
   public void testMultipleProjectionsIndexed() {
      Query<Object> query = indexedCache.query(
            "select concat(name, '!'), upper(name) from " + Game.ENTITY_NAME + " order by name");
      List<Object> results = query.list();
      assertThat(results).hasSize(3);
      Object[] r0 = (Object[]) results.get(0);
      assertThat(r0[0]).isEqualTo("Civilization!");
      assertThat(r0[1]).isEqualTo("CIVILIZATION");
      Object[] r1 = (Object[]) results.get(1);
      assertThat(r1[0]).isEqualTo("Doom!");
      assertThat(r1[1]).isEqualTo("DOOM");
      Object[] r2 = (Object[]) results.get(2);
      assertThat(r2[0]).isEqualTo("Tetris!");
      assertThat(r2[1]).isEqualTo("TETRIS");
   }

   @Test
   public void testLengthIndexed() {
      Query<Object> query = indexedCache.query(
            "select length(name) from " + Game.ENTITY_NAME + " order by name");
      List<Object> results = query.list();
      assertThat(results).hasSize(3);
      assertThat(((Object[]) results.get(0))[0]).isEqualTo(12);
      assertThat(((Object[]) results.get(1))[0]).isEqualTo(4);
      assertThat(((Object[]) results.get(2))[0]).isEqualTo(6);
   }

   @Test
   public void testUnknownFunctionIndexed() {
      // Function names are validated against the registry at parse time.
      assertThatThrownBy(() -> indexedCache.query(
            "select nonExistentFunc(name) from " + Game.ENTITY_NAME))
            .isInstanceOf(ParsingException.class)
            .hasMessageContaining("nonExistentFunc");
   }

   @Test
   public void testTrimIndexed() {
      Query<Object> query = indexedCache.query(
            "select trim(concat(' ', name, ' ')) from " + Game.ENTITY_NAME + " where name = 'Doom'");
      List<Object> results = query.list();
      assertThat(results).hasSize(1);
      assertThat(((Object[]) results.get(0))[0]).isEqualTo("Doom");
   }

   @Test
   public void testReplaceIndexed() {
      Query<Object> query = indexedCache.query(
            "select replace(description, 'game', 'classic') from " + Game.ENTITY_NAME + " where name = 'Tetris'");
      List<Object> results = query.list();
      assertThat(results).hasSize(1);
      assertThat(((Object[]) results.get(0))[0]).isEqualTo("Puzzle classic with blocks");
   }

   // --- Non-indexed path tests ---

   @Test
   public void testConcatNonIndexed() {
      Query<Object> query = nonIndexedCache.query(
            "select concat(name, ' - ', description) from " + Game.ENTITY_NAME + " order by name");
      List<Object> results = query.list();
      assertThat(results).hasSize(3);
      assertThat(((Object[]) results.get(0))[0]).isEqualTo("Civilization - The best strategy game");
      assertThat(((Object[]) results.get(1))[0]).isEqualTo("Doom - First person shooter classic");
      assertThat(((Object[]) results.get(2))[0]).isEqualTo("Tetris - Puzzle game with blocks");
   }

   @Test
   public void testUpperNonIndexed() {
      Query<Object> query = nonIndexedCache.query(
            "select upper(name) from " + Game.ENTITY_NAME + " order by name");
      List<Object> results = query.list();
      assertThat(results).hasSize(3);
      assertThat(((Object[]) results.get(0))[0]).isEqualTo("CIVILIZATION");
      assertThat(((Object[]) results.get(1))[0]).isEqualTo("DOOM");
      assertThat(((Object[]) results.get(2))[0]).isEqualTo("TETRIS");
   }

   @Test
   public void testNestedFunctionNonIndexed() {
      Query<Object> query = nonIndexedCache.query(
            "select upper(concat(name, '!')) from " + Game.ENTITY_NAME + " order by name");
      List<Object> results = query.list();
      assertThat(results).hasSize(3);
      assertThat(((Object[]) results.get(0))[0]).isEqualTo("CIVILIZATION!");
      assertThat(((Object[]) results.get(1))[0]).isEqualTo("DOOM!");
      assertThat(((Object[]) results.get(2))[0]).isEqualTo("TETRIS!");
   }

   @Test
   public void testWithWhereClauseNonIndexed() {
      Query<Object> query = nonIndexedCache.query(
            "select concat(name, '!') from " + Game.ENTITY_NAME + " where name = 'Doom'");
      List<Object> results = query.list();
      assertThat(results).hasSize(1);
      assertThat(((Object[]) results.get(0))[0]).isEqualTo("Doom!");
   }

   @Test
   public void testMultipleProjectionsNonIndexed() {
      Query<Object> query = nonIndexedCache.query(
            "select concat(name, '!'), upper(name) from " + Game.ENTITY_NAME + " order by name");
      List<Object> results = query.list();
      assertThat(results).hasSize(3);
      Object[] r0 = (Object[]) results.get(0);
      assertThat(r0[0]).isEqualTo("Civilization!");
      assertThat(r0[1]).isEqualTo("CIVILIZATION");
      Object[] r1 = (Object[]) results.get(1);
      assertThat(r1[0]).isEqualTo("Doom!");
      assertThat(r1[1]).isEqualTo("DOOM");
      Object[] r2 = (Object[]) results.get(2);
      assertThat(r2[0]).isEqualTo("Tetris!");
      assertThat(r2[1]).isEqualTo("TETRIS");
   }

   @Test
   public void testUnknownFunctionNonIndexed() {
      assertThatThrownBy(() -> nonIndexedCache.query(
            "select nonExistentFunc(name) from " + Game.ENTITY_NAME))
            .isInstanceOf(ParsingException.class)
            .hasMessageContaining("nonExistentFunc");
   }

   // --- Parameter tests ---

   @Test
   public void testFunctionWithParameterIndexed() {
      Query<Object> query = indexedCache.query(
            "select concat(name, ' - ', :label) from " + Game.ENTITY_NAME + " where name = :name");
      query.setParameter("label", "Game");
      query.setParameter("name", "Doom");
      List<Object> results = query.list();
      assertThat(results).hasSize(1);
      assertThat(((Object[]) results.get(0))[0]).isEqualTo("Doom - Game");
   }

   // --- Unified expression ladder tests (arithmetic, unary minus, parens) ---

   @Test
   public void testMultiplyProjectionIndexed() {
      Query<Object> query = indexedCache.query(
            "select price * 2 from " + Game.ENTITY_NAME + " where name = 'Tetris'");
      List<Object> results = query.list();
      assertThat(results).hasSize(1);
      assertThat((Double) ((Object[]) results.get(0))[0]).isEqualTo(39.98);
   }

   @Test
   public void testAdditionProjectionNonIndexed() {
      Query<Object> query = nonIndexedCache.query(
            "select price + 10 from " + Game.ENTITY_NAME + " where name = 'Tetris'");
      List<Object> results = query.list();
      assertThat(results).hasSize(1);
      assertThat((Double) ((Object[]) results.get(0))[0]).isEqualTo(29.99);
   }

   @Test
   public void testStringConcatProjectionIndexed() {
      Query<Object> query = indexedCache.query(
            "select name + '!' from " + Game.ENTITY_NAME + " where name = 'Doom'");
      List<Object> results = query.list();
      assertThat(results).hasSize(1);
      assertThat(((Object[]) results.get(0))[0]).isEqualTo("Doom!");
   }

   @Test
   public void testUnaryMinusProjectionIndexed() {
      Query<Object> query = indexedCache.query(
            "select -price from " + Game.ENTITY_NAME + " where name = 'Tetris'");
      List<Object> results = query.list();
      assertThat(results).hasSize(1);
      assertThat((Double) ((Object[]) results.get(0))[0]).isEqualTo(-19.99);
   }

   @Test
   public void testParenthesizedProjectionIndexed() {
      Query<Object> query = indexedCache.query(
            "select (price + 5) * 2 from " + Game.ENTITY_NAME + " where name = 'Tetris'");
      List<Object> results = query.list();
      assertThat(results).hasSize(1);
      assertThat((Double) ((Object[]) results.get(0))[0]).isEqualTo(49.98);
   }

   @Test
   public void testDivideAndModuloProjectionNonIndexed() {
      Query<Object> query = nonIndexedCache.query(
            "select price / 2 from " + Game.ENTITY_NAME + " where name = 'Tetris'");
      List<Object> results = query.list();
      assertThat(results).hasSize(1);
      assertThat((Double) ((Object[]) results.get(0))[0]).isEqualTo(9.995);

      Query<Object> modQuery = nonIndexedCache.query(
            "select rating % 3 from " + Game.ENTITY_NAME + " where name = 'Tetris'");
      List<Object> modResults = modQuery.list();
      assertThat(modResults).hasSize(1);
      assertThat(((Object[]) modResults.get(0))[0]).isEqualTo(2L);
   }

   @Test
   public void testMixedPropertyAndExpressionProjectionIndexed() {
      // A plain property and an expression projection in the same SELECT
      // must both be present in the result row.
      Query<Object> query = indexedCache.query(
            "select name, upper(name) from " + Game.ENTITY_NAME + " where name = 'Doom'");
      List<Object> results = query.list();
      assertThat(results).hasSize(1);
      Object[] row = (Object[]) results.get(0);
      assertThat(row[0]).isEqualTo("Doom");
      assertThat(row[1]).isEqualTo("DOOM");
   }

   @Test
   public void testMixedPropertyAndExpressionProjectionNonIndexed() {
      Query<Object> query = nonIndexedCache.query(
            "select price, price * 2 from " + Game.ENTITY_NAME + " where name = 'Tetris'");
      List<Object> results = query.list();
      assertThat(results).hasSize(1);
      Object[] row = (Object[]) results.get(0);
      assertThat((Double) row[0]).isEqualTo(19.99);
      assertThat((Double) row[1]).isEqualTo(39.98);
   }

   @Test
   public void testNullPropagationInProjectionIndexed() {
      // releaseDate is null for all games; the function must propagate null.
      Query<Object> query = indexedCache.query(
            "select upper(releaseDate) from " + Game.ENTITY_NAME + " where name = 'Doom'");
      List<Object> results = query.list();
      assertThat(results).hasSize(1);
      assertThat(((Object[]) results.get(0))[0]).isNull();
   }

   @Test
   public void testUnknownFunctionNestedInArithmeticRejectedAtParseTime() {
      assertThatThrownBy(() -> indexedCache.query(
            "select upper(nonExistentFunc(name)) from " + Game.ENTITY_NAME))
            .isInstanceOf(ParsingException.class)
            .hasMessageContaining("nonExistentFunc");
   }

   @Test
   public void testArithmeticWithParameterProjectionIndexed() {
      Query<Object> query = indexedCache.query(
            "select price + :discount from " + Game.ENTITY_NAME + " where name = 'Tetris'");
      query.setParameter("discount", 5.0);
      List<Object> results = query.list();
      assertThat(results).hasSize(1);
      assertThat((Double) ((Object[]) results.get(0))[0]).isEqualTo(24.99);
   }

   @Test
   @SuppressWarnings("unchecked")
   public void testExpressionProjectionQueryKeepsParametersAndPagingInSync() {
      // Build the query directly (bypassing the DelegatingQuery that normally rebuilds it) to check
      // that the settings are applied to the query itself, not only forwarded to the inner query.
      QueryEngine<Object> engine = ComponentRegistry.componentOf(nonIndexedCache.getAdvancedCache(), QueryEngine.class);
      Map<String, Object> params = new HashMap<>();
      params.put("suffix", "!");
      BaseQuery<Object> query = (BaseQuery<Object>) engine.buildQuery(engine.parse(
            "select concat(name, :suffix) from " + Game.ENTITY_NAME + " order by name"), params, 0, -1, false);

      assertThat(query.list()).extracting(r -> ((Object[]) r)[0]).containsExactly("Civilization!", "Doom!", "Tetris!");

      query.setParameter("suffix", "?");
      assertThat(query.getParameters()).containsEntry("suffix", "?");
      assertThat(query.list()).extracting(r -> ((Object[]) r)[0]).containsExactly("Civilization?", "Doom?", "Tetris?");

      query.setParameters(Map.of("suffix", "#"));
      assertThat(query.getParameters()).containsEntry("suffix", "#");
      assertThat(query.list()).extracting(r -> ((Object[]) r)[0]).containsExactly("Civilization#", "Doom#", "Tetris#");

      assertThatThrownBy(() -> query.setParameter("unknown", 1)).isInstanceOf(IllegalArgumentException.class);

      query.startOffset(1).maxResults(1);
      assertThat(query.getStartOffset()).isEqualTo(1);
      assertThat(query.getMaxResults()).isEqualTo(1);
      assertThat(query.list()).extracting(r -> ((Object[]) r)[0]).containsExactly("Doom#");
   }
}
