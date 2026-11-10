package org.infinispan.query.core.tests;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;
import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;

import java.time.LocalDateTime;
import java.util.Date;
import java.util.List;

import org.infinispan.Cache;
import org.infinispan.api.annotations.indexing.Indexed;
import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.commons.api.query.Query;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.protostream.sampledomain.Game;
import org.infinispan.query.objectfilter.ParsingException;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "query.core.tests.UpdateQueryCoreTest")
public class UpdateQueryCoreTest extends SingleCacheManagerTest {

   private static final String ENTITY = Game.class.getName();
   private static final String EVENT_ENTITY = Event.class.getName();

   private Cache<String, Game> gameCache;
   private Cache<Integer, Event> eventCache;

   /**
    * Plain (reflection-based) entity, used to exercise the parse-time type
    * checks against a non-protobuf target. Temporal fields of protobuf
    * entities are not exposed as a primitive type by the metadata, so the
    * Date-typed target is only reachable through a reflection entity.
    */
   @Indexed
   public static class Event {
      public String title;
      public Date when;
      public int count;
      public boolean featured;

      public Event() {
      }

      public Event(String title) {
         this.title = title;
      }
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() {
      ConfigurationBuilder builder = getDefaultStandaloneCacheConfig(false);
      builder.encoding().mediaType(MediaType.APPLICATION_PROTOSTREAM_TYPE)
            .indexing().enable().storage(LOCAL_HEAP)
            .addIndexedEntity(ENTITY);

      ConfigurationBuilder eventBuilder = getDefaultStandaloneCacheConfig(false);
      eventBuilder.indexing().enable().storage(LOCAL_HEAP)
            .addIndexedEntity(EVENT_ENTITY);

      cacheManager = TestCacheManagerFactory.createCacheManager(Game.GameSchema.INSTANCE, null);
      gameCache = cacheManager.administration()
            .withFlags(CacheContainerAdmin.AdminFlag.VOLATILE)
            .getOrCreateCache("update-test", builder.build());
      eventCache = cacheManager.administration()
            .withFlags(CacheContainerAdmin.AdminFlag.VOLATILE)
            .getOrCreateCache("update-test-reflection", eventBuilder.build());
      return cacheManager;
   }

   @BeforeMethod
   public void populateCache() {
      gameCache.clear();
      gameCache.putAll(Game.data());
      eventCache.clear();
      eventCache.put(1, new Event("Alpha"));
      eventCache.put(2, new Event("Beta"));
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

   @Test
   public void testUpdateWithNamedParameter() {
      Query<Game> update = gameCache.query(
            "update from " + ENTITY + " set name = :newName where name = :oldName");
      update.setParameter("newName", "Civilization VII");
      update.setParameter("oldName", "Civilization");
      int count = update.executeStatement();

      assertThat(count).isEqualTo(1);

      Game result = gameCache.get("g1");
      assertThat(result.getName()).isEqualTo("Civilization VII");
      assertThat(result.getDescription()).isEqualTo("The best strategy game");
   }

   @Test
   public void testCacheGetAfterUpdate() {
      gameCache.query("update from " + ENTITY + " set name = 'Civ VI' where name = 'Civilization'")
            .executeStatement();

      Game result = gameCache.get("g1");
      assertThat(result.getName()).isEqualTo("Civ VI");
      assertThat(result.getDescription()).isEqualTo("The best strategy game");
      assertThat(gameCache.size()).isEqualTo(3);
   }

   // --- Expression tests ---

   @Test
   public void testExpressionRatingIncrement() {
      Query<Game> update = gameCache.query(
            "update from " + ENTITY + " set rating = rating + 1 where name = 'Civilization'");
      int count = update.executeStatement();

      assertThat(count).isEqualTo(1);
      assertThat(gameCache.get("g1").getRating()).isEqualTo(91);
   }

   @Test
   public void testExpressionRatingDecrement() {
      Query<Game> update = gameCache.query(
            "update from " + ENTITY + " set rating = rating - 5 where name = 'Doom'");
      int count = update.executeStatement();

      assertThat(count).isEqualTo(1);
      assertThat(gameCache.get("g2").getRating()).isEqualTo(80);
   }

   @Test
   public void testExpressionRatingMultiply() {
      Query<Game> update = gameCache.query(
            "update from " + ENTITY + " set rating = rating * 2 where name = 'Tetris'");
      int count = update.executeStatement();

      assertThat(count).isEqualTo(1);
      assertThat(gameCache.get("g3").getRating()).isEqualTo(190);
   }

   @Test
   public void testExpressionStringConcatenation() {
      Query<Game> update = gameCache.query(
            "update from " + ENTITY + " set name = name + ' Remastered' where name = 'Doom'");
      int count = update.executeStatement();

      assertThat(count).isEqualTo(1);
      assertThat(gameCache.get("g2").getName()).isEqualTo("Doom Remastered");
   }

   @Test
   public void testExpressionFunctionUpper() {
      Query<Game> update = gameCache.query(
            "update from " + ENTITY + " set name = upper(name) where name = 'Doom'");
      int count = update.executeStatement();

      assertThat(count).isEqualTo(1);
      assertThat(gameCache.get("g2").getName()).isEqualTo("DOOM");
   }

   @Test
   public void testExpressionFunctionConcat() {
      Query<Game> update = gameCache.query(
            "update from " + ENTITY + " set name = concat(name, ' - ', description) where name = 'Civilization'");
      int count = update.executeStatement();

      assertThat(count).isEqualTo(1);
      assertThat(gameCache.get("g1").getName()).isEqualTo("Civilization - The best strategy game");
   }

   @Test
   public void testExpressionWithParameter() {
      Query<Game> update = gameCache.query(
            "update from " + ENTITY + " set rating = rating + :increment where name = 'Civilization'");
      update.setParameter("increment", 10);
      int count = update.executeStatement();

      assertThat(count).isEqualTo(1);
      assertThat(gameCache.get("g1").getRating()).isEqualTo(100);
   }

   @Test
   public void testExpressionMultipleOperations() {
      Query<Game> update = gameCache.query(
            "update from " + ENTITY + " set rating = rating + 1, set description = concat(description, ' +1') where name = 'Civilization'");
      int count = update.executeStatement();

      assertThat(count).isEqualTo(1);
      Game g = gameCache.get("g1");
      assertThat(g.getRating()).isEqualTo(91);
      assertThat(g.getDescription()).isEqualTo("The best strategy game +1");
   }

   @Test
   public void testExpressionPreImageSemantics() {
      // Pre-image semantics: both right-hand sides are evaluated against the
      // original entry, so price uses the original rating (90), not the
      // already-incremented one (91). rating: 90 -> 91, price: 90 * 2 = 180.0.
      Query<Game> update = gameCache.query(
            "update from " + ENTITY + " set rating = rating + 1, set price = rating * 2 where name = 'Civilization'");
      int count = update.executeStatement();

      assertThat(count).isEqualTo(1);
      Game g = gameCache.get("g1");
      assertThat(g.getRating()).isEqualTo(91);
      assertThat(g.getPrice()).isEqualTo(180.0);
   }

   @Test
   public void testSetNonIntegralDoubleToIntFieldRejected() {
      // 7.0 / 2 = 3.5 is non-integral; rating is an int field, so the
      // assignment is rejected at parse time with a clear error.
      assertThatThrownBy(() -> gameCache.query(
            "update from " + ENTITY + " set rating = 7.0 / 2 where name = 'Civilization'"))
            .isInstanceOf(ParsingException.class)
            .hasMessageContaining("not integral");
      assertThat(gameCache.get("g1").getRating()).isEqualTo(90);
   }

   @Test
   public void testSetIntegralDoubleToIntFieldConverted() {
      // 3.0 is integral, so it converts to the int field without error.
      Query<Game> update = gameCache.query(
            "update from " + ENTITY + " set rating = 3.0 where name = 'Civilization'");
      int count = update.executeStatement();
      assertThat(count).isEqualTo(1);
      assertThat(gameCache.get("g1").getRating()).isEqualTo(3);
   }

   @Test
   public void testSetBooleanToIntegerFieldRejectedAtParseTime() {
      // Booleans can never be converted to numbers at runtime, so the
      // parse-time checker rejects it with a precise error.
      assertThatThrownBy(() -> gameCache.query(
            "update from " + ENTITY + " set rating = true where name = 'Civilization'"))
            .isInstanceOf(ParsingException.class)
            .hasMessageContaining("ISPN028537");
      assertThat(gameCache.get("g1").getRating()).isEqualTo(90);
   }

   @Test
   public void testSetNumberToTemporalFieldRejectedAtParseTime() {
      assertThatThrownBy(() -> eventCache.query(
            "update from " + EVENT_ENTITY + " set when = 1"))
            .isInstanceOf(ParsingException.class)
            .hasMessageContaining("ISPN028537");
   }

   @Test
   public void testSetStringToTemporalFieldRejectedAtParseTime() {
      // The runtime has no string -> temporal conversion.
      assertThatThrownBy(() -> eventCache.query(
            "update from " + EVENT_ENTITY + " set when = '2026-01-01'"))
            .isInstanceOf(ParsingException.class)
            .hasMessageContaining("ISPN028537");
   }

   @Test
   public void testSetFunctionResultToTemporalFieldRejectedAtParseTime() {
      // upper() declares a String return type; a string cannot be assigned
      // to a temporal field.
      assertThatThrownBy(() -> eventCache.query(
            "update from " + EVENT_ENTITY + " set when = upper(title)"))
            .isInstanceOf(ParsingException.class)
            .hasMessageContaining("ISPN028537");
   }

   @Test
   public void testUpdateReflectionEntity() {
      // Positive control: the reflection apply path works for expressions.
      Event event = eventCache.get(1);
      event.count = 5;
      eventCache.put(1, event);
      Query<Event> update = eventCache.query(
            "update from " + EVENT_ENTITY + " set count = count + 1");
      int count = update.executeStatement();
      assertThat(count).isEqualTo(2);
      assertThat(eventCache.get(1).count).isEqualTo(6);
      assertThat(eventCache.get(2).count).isEqualTo(1);
   }

   @Test
   public void testAtomicityPartialFailureLeavesEntryUnchanged() {
      // Op 1 (title) applies successfully; op 2 (count) fails at apply time
      // because 'abc' cannot be converted to int. The statement must fail
      // and the entries must be left bit-for-bit unchanged.
      assertThatThrownBy(() -> eventCache.query(
                  "update from " + EVENT_ENTITY + " set title = 'changed', set count = 'abc'")
            .executeStatement())
            .isInstanceOf(RuntimeException.class);
      assertThat(eventCache.get(1).title).isEqualTo("Alpha");
      assertThat(eventCache.get(1).count).isEqualTo(0);
      assertThat(eventCache.get(2).title).isEqualTo("Beta");
      assertThat(eventCache.get(2).count).isEqualTo(0);
   }

   @Test
   public void testAtomicityExpressionPartialFailureLeavesEntryUnchanged() {
      // Op 1 (title) applies successfully; op 2's value passes parse-time
      // checks (string to int is deferred to the runtime) but fails
      // conversion at apply time. The entries must be left unchanged.
      assertThatThrownBy(() -> eventCache.query(
                  "update from " + EVENT_ENTITY + " set title = 'changed', set count = tostring('abc')")
            .executeStatement())
            .isInstanceOf(RuntimeException.class);
      assertThat(eventCache.get(1).title).isEqualTo("Alpha");
      assertThat(eventCache.get(1).count).isEqualTo(0);
      assertThat(eventCache.get(2).title).isEqualTo("Beta");
      assertThat(eventCache.get(2).count).isEqualTo(0);
   }

   @Test
   public void testSetArithmeticOnNonNumericRejectedAtParseTime() {
      // Subtraction requires numeric operands; name is a String.
      assertThatThrownBy(() -> gameCache.query(
            "update from " + ENTITY + " set rating = name - 1 where name = 'Civilization'"))
            .isInstanceOf(ParsingException.class)
            .hasMessageContaining("ISPN028538");
   }

   @Test
   public void testSetConstantDivisionByZeroRejectedAtParseTime() {
      assertThatThrownBy(() -> gameCache.query(
            "update from " + ENTITY + " set rating = 1 / 0 where name = 'Civilization'"))
            .isInstanceOf(ParsingException.class)
            .hasMessageContaining("ISPN028540");
   }

   @Test
   public void testSetConstantDivisionByFractionAccepted() {
      // The divisor is non-zero (0.5): it must not be truncated to 0 and rejected as a division by zero.
      Query<Game> update = gameCache.query(
            "update from " + ENTITY + " set price = 10 / 0.5 where name = 'Civilization'");
      assertThat(update.executeStatement()).isEqualTo(1);
      assertThat(gameCache.get("g1").getPrice()).isEqualTo(20.0);

      update = gameCache.query(
            "update from " + ENTITY + " set price = 10.75 % 0.5 where name = 'Civilization'");
      assertThat(update.executeStatement()).isEqualTo(1);
      assertThat(gameCache.get("g1").getPrice()).isEqualTo(0.25);
   }

   @Test
   public void testSetConstantDoubleDivisionByZeroRejectedAtParseTime() {
      assertThatThrownBy(() -> gameCache.query(
            "update from " + ENTITY + " set price = 1.5 / 0.0 where name = 'Civilization'"))
            .isInstanceOf(ParsingException.class)
            .hasMessageContaining("ISPN028540");
   }

   @Test
   public void testNegatedNamedParameter() {
      Query<Game> update = gameCache.query(
            "update from " + ENTITY + " set rating = -:delta where name = 'Civilization'");
      update.setParameter("delta", 5);
      assertThat(update.executeStatement()).isEqualTo(1);
      assertThat(gameCache.get("g1").getRating()).isEqualTo(-5);
   }

   @Test
   public void testNegatedNamedParameterInExpression() {
      Query<Game> update = gameCache.query(
            "update from " + ENTITY + " set rating = rating + -:delta where name = 'Civilization'");
      update.setParameter("delta", 5);
      assertThat(update.executeStatement()).isEqualTo(1);
      assertThat(gameCache.get("g1").getRating()).isEqualTo(85);
   }

   @Test
   public void testSetOutOfRangeValueToIntFieldRejectedAtParseTime() {
      // 3000000000 does not fit in an int: it must be rejected instead of silently wrapping.
      assertThatThrownBy(() -> gameCache.query(
            "update from " + ENTITY + " set rating = 3000000000 where name = 'Civilization'"))
            .isInstanceOf(ParsingException.class)
            .hasMessageContaining("ISPN028541");
      assertThat(gameCache.get("g1").getRating()).isEqualTo(90);
   }

   @Test
   public void testExpressionOverflowingIntFieldRejected() {
      Query<Game> update = gameCache.query(
            "update from " + ENTITY + " set rating = rating * 100000000 where name = 'Civilization'");
      assertThatThrownBy(update::executeStatement).hasStackTraceContaining("out of range");
      assertThat(gameCache.get("g1").getRating()).isEqualTo(90);
   }

   @Test
   public void testSetMaxIntToIntFieldAccepted() {
      Query<Game> update = gameCache.query(
            "update from " + ENTITY + " set rating = 2147483647 where name = 'Civilization'");
      assertThat(update.executeStatement()).isEqualTo(1);
      assertThat(gameCache.get("g1").getRating()).isEqualTo(Integer.MAX_VALUE);
   }

   @Test
   public void testSetUnknownTargetPropertyRejectedAtParseTime() {
      // Typo in the SET target: rejected at parse time instead of failing
      // per entry at runtime.
      assertThatThrownBy(() -> gameCache.query(
            "update from " + ENTITY + " set rting = 1 where name = 'Civilization'"))
            .isInstanceOf(ParsingException.class)
            .hasMessageContaining("ISPN028501");
   }

   @Test
   public void testSetStringToIntegerFieldDeferredToRuntime() {
      // '42' is not rejected at parse time: the runtime converts it via
      // Util.fromString.
      Query<Game> update = gameCache.query(
            "update from " + ENTITY + " set rating = '42' where name = 'Civilization'");
      int count = update.executeStatement();
      assertThat(count).isEqualTo(1);
      assertThat(gameCache.get("g1").getRating()).isEqualTo(42);
   }

   @Test
   public void testSetNumberToStringFieldCoercedAtRuntime() {
      // A number assigned to a String field is tolerated at parse time and
      // converted at runtime.
      Query<Game> update = gameCache.query(
            "update from " + ENTITY + " set name = 1 where name = 'Civilization'");
      int count = update.executeStatement();
      assertThat(count).isEqualTo(1);
      assertThat(gameCache.get("g1").getName()).isEqualTo("1");
   }

   @Test
   public void testExpressionArithmeticPrecedence() {
      // rating = 90, set rating = rating + 2 * 3 = 90 + 6 = 96
      Query<Game> update = gameCache.query(
            "update from " + ENTITY + " set rating = rating + 2 * 3 where name = 'Civilization'");
      int count = update.executeStatement();

      assertThat(count).isEqualTo(1);
      assertThat(gameCache.get("g1").getRating()).isEqualTo(96);
   }

   @Test
   public void testExpressionParenthesizedExpression() {
      // rating = 90, set rating = (rating + 2) * 3 = 92 * 3 = 276
      Query<Game> update = gameCache.query(
            "update from " + ENTITY + " set rating = (rating + 2) * 3 where name = 'Civilization'");
      int count = update.executeStatement();

      assertThat(count).isEqualTo(1);
      assertThat(gameCache.get("g1").getRating()).isEqualTo(276);
   }

   @Test
   public void testExpressionDivision() {
      Query<Game> update = gameCache.query(
            "update from " + ENTITY + " set rating = rating / 2 where name = 'Tetris'");
      int count = update.executeStatement();

      assertThat(count).isEqualTo(1);
      assertThat(gameCache.get("g3").getRating()).isEqualTo(47);
   }

   @Test
   public void testExpressionModulo() {
      Query<Game> update = gameCache.query(
            "update from " + ENTITY + " set rating = rating % 7 where name = 'Tetris'");
      int count = update.executeStatement();

      assertThat(count).isEqualTo(1);
      assertThat(gameCache.get("g3").getRating()).isEqualTo(4);
   }

   @Test
   public void testExpressionPropertyReferenceCopy() {
      Query<Game> update = gameCache.query(
            "update from " + ENTITY + " set description = name where name = 'Doom'");
      int count = update.executeStatement();

      assertThat(count).isEqualTo(1);
      assertThat(gameCache.get("g2").getDescription()).isEqualTo("Doom");
   }

   // --- Double field expression tests ---

   @Test
   public void testExpressionDoubleAddition() {
      Query<Game> update = gameCache.query(
            "update from " + ENTITY + " set price = price + 10.5 where name = 'Civilization'");
      int count = update.executeStatement();

      assertThat(count).isEqualTo(1);
      assertThat(gameCache.get("g1").getPrice()).isCloseTo(70.49, within(1e-10));
   }

   @Test
   public void testExpressionDoubleMultiplication() {
      Query<Game> update = gameCache.query(
            "update from " + ENTITY + " set price = price * 1.5 where name = 'Doom'");
      int count = update.executeStatement();

      assertThat(count).isEqualTo(1);
      assertThat(gameCache.get("g2").getPrice()).isCloseTo(59.985, within(1e-10));
   }

   @Test
   public void testExpressionRound() {
      Query<Game> update = gameCache.query(
            "update from " + ENTITY + " set rating = round(price) where name = 'Civilization'");
      int count = update.executeStatement();

      assertThat(count).isEqualTo(1);
      assertThat(gameCache.get("g1").getRating()).isEqualTo(60);
   }

   @Test
   public void testExpressionFloor() {
      Query<Game> update = gameCache.query(
            "update from " + ENTITY + " set price = floor(price) where name = 'Tetris'");
      int count = update.executeStatement();

      assertThat(count).isEqualTo(1);
      assertThat(gameCache.get("g3").getPrice()).isEqualTo(19.0);
   }

   @Test
   public void testExpressionCeil() {
      Query<Game> update = gameCache.query(
            "update from " + ENTITY + " set price = ceil(price) where name = 'Doom'");
      int count = update.executeStatement();

      assertThat(count).isEqualTo(1);
      assertThat(gameCache.get("g2").getPrice()).isEqualTo(40.0);
   }

   // --- ADD/REMOVE collection tests ---

   @Test
   public void testAddSingleValue() {
      Query<Game> update = gameCache.query(
            "update from " + ENTITY + " add categories = 'rpg' where name = 'Civilization'");
      int count = update.executeStatement();

      assertThat(count).isEqualTo(1);
      assertThat(gameCache.get("g1").getCategories()).containsExactly("strategy", "turn-based", "4x", "rpg");
   }

   @Test
   public void testAddMultipleValues() {
      Query<Game> update = gameCache.query(
            "update from " + ENTITY + " add categories = ('rpg', 'sandbox') where name = 'Doom'");
      int count = update.executeStatement();

      assertThat(count).isEqualTo(1);
      assertThat(gameCache.get("g2").getCategories()).containsExactly("shooter", "action", "fps", "rpg", "sandbox");
   }

   @Test
   public void testRemoveSingleValue() {
      Query<Game> update = gameCache.query(
            "update from " + ENTITY + " remove categories = 'turn-based' where name = 'Civilization'");
      int count = update.executeStatement();

      assertThat(count).isEqualTo(1);
      assertThat(gameCache.get("g1").getCategories()).containsExactly("strategy", "4x");
   }

   @Test
   public void testRemoveMultipleValues() {
      Query<Game> update = gameCache.query(
            "update from " + ENTITY + " remove categories = ('action', 'fps') where name = 'Doom'");
      int count = update.executeStatement();

      assertThat(count).isEqualTo(1);
      assertThat(gameCache.get("g2").getCategories()).containsExactly("shooter");
   }

   @Test
   public void testAddAndRemove() {
      Query<Game> update = gameCache.query(
            "update from " + ENTITY + " add categories = 'puzzle', remove categories = 'arcade' where name = 'Tetris'");
      int count = update.executeStatement();

      assertThat(count).isEqualTo(1);
      assertThat(gameCache.get("g3").getCategories()).containsExactly("puzzle", "classic", "puzzle");
   }

   @Test
   public void testAddWithExpression() {
      Query<Game> update = gameCache.query(
            "update from " + ENTITY + " add categories = concat('rating-', toLong(rating)) where name = 'Civilization'");
      int count = update.executeStatement();

      assertThat(count).isEqualTo(1);
      assertThat(gameCache.get("g1").getCategories()).contains("rating-90");
   }

   @Test
   public void testAddDoesNotAffectNonMatching() {
      Query<Game> update = gameCache.query(
            "update from " + ENTITY + " add categories = 'new' where name = 'nonexistent'");
      int count = update.executeStatement();

      assertThat(count).isEqualTo(0);
      assertThat(gameCache.get("g1").getCategories()).containsExactly("strategy", "turn-based", "4x");
      assertThat(gameCache.get("g2").getCategories()).containsExactly("shooter", "action", "fps");
      assertThat(gameCache.get("g3").getCategories()).containsExactly("puzzle", "arcade", "classic");
   }

   @Test
   public void testMixedSetAndAdd() {
      Query<Game> update = gameCache.query(
            "update from " + ENTITY + " set rating = 99, add categories = 'updated' where name = 'Civilization'");
      int count = update.executeStatement();

      assertThat(count).isEqualTo(1);
      Game g = gameCache.get("g1");
      assertThat(g.getRating()).isEqualTo(99);
      assertThat(g.getCategories()).containsExactly("strategy", "turn-based", "4x", "updated");
   }

   @Test
   public void testMixedSetAndRemove() {
      Query<Game> update = gameCache.query(
            "update from " + ENTITY + " set rating = 50, remove categories = 'fps' where name = 'Doom'");
      int count = update.executeStatement();

      assertThat(count).isEqualTo(1);
      Game g = gameCache.get("g2");
      assertThat(g.getRating()).isEqualTo(50);
      assertThat(g.getCategories()).containsExactly("shooter", "action");
   }

   @Test
   public void testMixedAddAndAdd() {
      Query<Game> update = gameCache.query(
            "update from " + ENTITY + " add categories = 'rpg', add categories = ('sandbox', 'open-world') where name = 'Tetris'");
      int count = update.executeStatement();

      assertThat(count).isEqualTo(1);
      assertThat(gameCache.get("g3").getCategories())
            .containsExactly("puzzle", "arcade", "classic", "rpg", "sandbox", "open-world");
   }

   @Test
   public void testMixedRemoveAndRemove() {
      Query<Game> update = gameCache.query(
            "update from " + ENTITY + " remove categories = 'strategy', remove categories = ('turn-based', '4x') where name = 'Civilization'");
      int count = update.executeStatement();

      assertThat(count).isEqualTo(1);
      assertThat(gameCache.get("g1").getCategories()).isEmpty();
   }

   @Test
   public void testMixedSetAddRemove() {
      Query<Game> update = gameCache.query(
            "update from " + ENTITY + " set rating = 100, add categories = 'russian', remove categories = 'arcade' where name = 'Tetris'");
      int count = update.executeStatement();

      assertThat(count).isEqualTo(1);
      Game g = gameCache.get("g3");
      assertThat(g.getRating()).isEqualTo(100);
      assertThat(g.getCategories()).containsExactly("puzzle", "classic", "russian");
   }

   @Test
   public void testRegexpReplaceDescription() {
      Query<Game> update = gameCache.query(
            "update from " + ENTITY + " set description = regexp_replace(description, 'game', 'title') where name = 'Tetris'");
      int count = update.executeStatement();

      assertThat(count).isEqualTo(1);
      assertThat(gameCache.get("g3").getDescription()).isEqualTo("Puzzle title with blocks");
   }

   @Test
   public void testRegexpReplaceMultipleOccurrences() {
      Query<Game> update = gameCache.query(
            "update from " + ENTITY + " set description = regexp_replace(description, 's', 'S') where name = 'Doom'");
      int count = update.executeStatement();

      assertThat(count).isEqualTo(1);
      assertThat(gameCache.get("g2").getDescription()).isEqualTo("FirSt perSon Shooter claSSic");
   }

   @Test
   public void testRegexpSubstrNoMatch() {
      Query<Game> update = gameCache.query(
            "update from " + ENTITY + " set description = regexp_substr(description, '[0-9]+') where name = 'Doom'");
      int count = update.executeStatement();

      assertThat(count).isEqualTo(1);
      assertThat(gameCache.get("g2").getDescription()).isNull();
   }

   @Test
   public void testRegexpInstr() {
      Query<Game> update = gameCache.query(
            "update from " + ENTITY + " set rating = regexp_instr(description, 'shooter') where name = 'Doom'");
      int count = update.executeStatement();

      assertThat(count).isEqualTo(1);
      assertThat(gameCache.get("g2").getRating()).isEqualTo(14);
   }

   // --- Date function tests ---

   @Test
   public void testReleaseDateRoundTrip() {
      Game g = gameCache.get("g2");
      assertThat(g.getReleaseDate()).isEqualTo(LocalDateTime.of(1993, 3, 10, 0, 0));
   }

   @Test
   public void testDateAddDays() {
      // Doom release: 1993-03-10, add 30 days → 1993-04-09
      Query<Game> update = gameCache.query(
            "update from " + ENTITY + " set releaseDate = adddays(releaseDate, 30) where name = 'Doom'");
      int count = update.executeStatement();

      assertThat(count).isEqualTo(1);
      assertThat(gameCache.get("g2").getReleaseDate()).isEqualTo(LocalDateTime.of(1993, 4, 9, 0, 0));
   }

   @Test
   public void testDateAddHours() {
      // Tetris release: 1984-06-06T00:00:00Z, add 90 hours → 1984-06-09T18:00:00Z
      Query<Game> update = gameCache.query(
            "update from " + ENTITY + " set releaseDate = addhours(releaseDate, 90) where name = 'Tetris'");
      int count = update.executeStatement();

      assertThat(count).isEqualTo(1);
      assertThat(gameCache.get("g3").getReleaseDate()).isEqualTo(LocalDateTime.of(1984, 6, 9, 18, 0, 0));
   }

   @Test
   public void testDateAddMinutes() {
      // Civilization release: 1991-09-15T00:00, add 90 minutes → 1991-09-15T01:30
      Query<Game> update = gameCache.query(
            "update from " + ENTITY + " set releaseDate = addminutes(releaseDate, 90) where name = 'Civilization'");
      int count = update.executeStatement();

      assertThat(count).isEqualTo(1);
      assertThat(gameCache.get("g1").getReleaseDate()).isEqualTo(LocalDateTime.of(1991, 9, 15, 1, 30, 0));
   }

   @Test
   public void testDateAddMonths() {
      // Tetris release: 1984-06-06, add 6 months → 1984-12-06
      Query<Game> update = gameCache.query(
            "update from " + ENTITY + " set releaseDate = addmonths(releaseDate, 6) where name = 'Tetris'");
      int count = update.executeStatement();

      assertThat(count).isEqualTo(1);
      assertThat(gameCache.get("g3").getReleaseDate()).isEqualTo(LocalDateTime.of(1984, 12, 6, 0, 0));
   }

   @Test
   public void testDateAddYears() {
      // Doom release: 1993-03-10, add 30 years → 2023-03-10 (anniversary)
      Query<Game> update = gameCache.query(
            "update from " + ENTITY + " set releaseDate = addyears(releaseDate, 30) where name = 'Doom'");
      int count = update.executeStatement();

      assertThat(count).isEqualTo(1);
      assertThat(gameCache.get("g2").getReleaseDate()).isEqualTo(LocalDateTime.of(2023, 3, 10, 0, 0));
   }

   @Test
   public void testDateAddWithUnitString() {
      // Civilization release: 1991-09-15, add 2 weeks → 1991-09-29
      Query<Game> update = gameCache.query(
            "update from " + ENTITY + " set releaseDate = dateadd(releaseDate, 2, 'weeks') where name = 'Civilization'");
      int count = update.executeStatement();

      assertThat(count).isEqualTo(1);
      assertThat(gameCache.get("g1").getReleaseDate()).isEqualTo(LocalDateTime.of(1991, 9, 29, 0, 0));
   }

   @Test
   public void testDateAddNegativeDays() {
      // Doom release: 1993-03-10, subtract 10 days → 1993-03-00? No: 1993-02-28
      Query<Game> update = gameCache.query(
            "update from " + ENTITY + " set releaseDate = adddays(releaseDate, -10) where name = 'Doom'");
      int count = update.executeStatement();

      assertThat(count).isEqualTo(1);
      assertThat(gameCache.get("g2").getReleaseDate()).isEqualTo(LocalDateTime.of(1993, 2, 28, 0, 0));
   }

   @Test
   public void testDateAddNullField() {
      // Set releaseDate to null first, then adddays should return null
      gameCache.put("g4", new Game("Minecraft", "Block game", 92, 29.99,
            List.of("sandbox", "survival"), null));

      Query<Game> update = gameCache.query(
            "update from " + ENTITY + " set releaseDate = adddays(releaseDate, 5) where name = 'Minecraft'");
      int count = update.executeStatement();

      assertThat(count).isEqualTo(1);
      assertThat(gameCache.get("g4").getReleaseDate()).isNull();
   }

   @Test
   public void testDateSetFromParameter() {
      Query<Game> update = gameCache.query(
            "update from " + ENTITY + " set releaseDate = :newDate where name = 'Tetris'");
      update.setParameter("newDate", LocalDateTime.of(1989, 12, 31, 0, 0));
      int count = update.executeStatement();

      assertThat(count).isEqualTo(1);
      assertThat(gameCache.get("g3").getReleaseDate()).isEqualTo(LocalDateTime.of(1989, 12, 31, 0, 0));
   }
}
