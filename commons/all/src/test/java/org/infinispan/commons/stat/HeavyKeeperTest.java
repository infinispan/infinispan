package org.infinispan.commons.stat;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.ToLongBiFunction;

import org.junit.jupiter.api.Test;

public class HeavyKeeperTest {

   private static final ToLongBiFunction<String, Integer> HASH = (key, seed) -> {
      long h = seed ^ 0x9E3779B97F4A7C15L;
      for (int i = 0; i < key.length(); i++) {
         h = h * 31 + key.charAt(i);
      }
      return h;
   };

   @Test
   public void testResetClearsSketchAndTopK() {
      HeavyKeeper<String> hk = new HeavyKeeper<>(3, 8, 7, 0.9, HASH);

      hk.incrBy("a", 100);
      hk.incrBy("b", 50);
      hk.incrBy("c", 25);

      assertThat(hk.query("a")).isTrue();
      assertThat(hk.list()).hasSize(3);

      hk.reset();

      assertThat(hk.query("a")).isFalse();
      assertThat(hk.query("b")).isFalse();
      assertThat(hk.query("c")).isFalse();
      assertThat(hk.getCount("a")).isEqualTo(0);
      assertThat(hk.list()).isEmpty();
   }

   @Test
   public void testResetAllowsReuse() {
      HeavyKeeper<String> hk = new HeavyKeeper<>(2, 8, 7, 0.9, HASH);

      hk.incrBy("a", 100);
      hk.incrBy("b", 50);
      hk.reset();

      hk.incrBy("x", 200);
      hk.incrBy("y", 100);

      assertThat(hk.query("a")).isFalse();
      assertThat(hk.query("x")).isTrue();
      assertThat(hk.query("y")).isTrue();
      assertThat(hk.getCount("x")).isEqualTo(200);
   }

   @Test
   public void testInvalidDecayRejected() {
      assertThatThrownBy(() -> new HeavyKeeper<>(3, 8, 7, 0.0, HASH))
            .isInstanceOf(IllegalArgumentException.class);

      assertThatThrownBy(() -> new HeavyKeeper<>(3, 8, 7, 1.0, HASH))
            .isInstanceOf(IllegalArgumentException.class);

      assertThatThrownBy(() -> new HeavyKeeper<>(3, 8, 7, -0.5, HASH))
            .isInstanceOf(IllegalArgumentException.class);

      assertThatThrownBy(() -> new HeavyKeeper<>(3, 8, 7, 1.5, HASH))
            .isInstanceOf(IllegalArgumentException.class);
   }

   @Test
   public void testHeavyHitterSurvivesAmongNoise() {
      HeavyKeeper<String> hk = new HeavyKeeper<>(3, 8, 7, 0.9, HASH);

      hk.incrBy("heavy", 500);

      for (int i = 0; i < 100; i++) {
         hk.add("noise-" + i);
      }

      assertThat(hk.query("heavy")).isTrue();
      assertThat(hk.getCount("heavy")).isEqualTo(500);
   }

   @Test
   public void testMultipleHeavyHittersAmongNoise() {
      HeavyKeeper<String> hk = new HeavyKeeper<>(3, 16, 7, 0.9, HASH);

      hk.incrBy("hot-1", 1000);
      hk.incrBy("hot-2", 800);
      hk.incrBy("hot-3", 600);

      for (int i = 0; i < 200; i++) {
         hk.add("noise-" + i);
      }

      assertThat(hk.query("hot-1")).isTrue();
      assertThat(hk.query("hot-2")).isTrue();
      assertThat(hk.query("hot-3")).isTrue();

      List<HeavyKeeper.KeyFrequency<String>> top = hk.list();
      assertThat(top.get(0).key()).isEqualTo("hot-1");
      assertThat(top.get(1).key()).isEqualTo("hot-2");
      assertThat(top.get(2).key()).isEqualTo("hot-3");
   }

   @Test
   public void testEvictionAndRePromotion() {
      HeavyKeeper<String> hk = new HeavyKeeper<>(2, 8, 7, 0.9, HASH);

      hk.incrBy("a", 10);
      hk.incrBy("b", 5);

      assertThat(hk.query("a")).isTrue();
      assertThat(hk.query("b")).isTrue();

      // "c" with a high count evicts "b" (the minimum).
      String expelled = hk.incrBy("c", 50);
      assertThat(expelled).isEqualTo("b");
      assertThat(hk.query("c")).isTrue();
      assertThat(hk.query("b")).isFalse();

      // "b" comes back with an even higher count, evicting "a".
      expelled = hk.incrBy("b", 100);
      assertThat(expelled).isEqualTo("a");
      assertThat(hk.query("b")).isTrue();
      assertThat(hk.query("a")).isFalse();
   }

   @Test
   public void testExpelledItemIdentity() {
      HeavyKeeper<String> hk = new HeavyKeeper<>(2, 8, 7, 0.9, HASH);

      hk.incrBy("keep", 100);
      hk.incrBy("victim", 1);

      String expelled = hk.incrBy("newcomer", 50);

      assertThat(expelled).isEqualTo("victim");
      assertThat(hk.query("keep")).isTrue();
      assertThat(hk.query("newcomer")).isTrue();
      assertThat(hk.query("victim")).isFalse();
   }

   @Test
   public void testCountIsExactAfterPromotion() {
      HeavyKeeper<String> hk = new HeavyKeeper<>(3, 8, 7, 0.9, HASH);

      hk.add("x");
      assertThat(hk.getCount("x")).isEqualTo(1);

      hk.incrBy("x", 5);
      assertThat(hk.getCount("x")).isEqualTo(6);

      hk.incrBy("x", 10);
      assertThat(hk.getCount("x")).isEqualTo(16);
   }

   @Test
   public void testListOrderStableAfterIncrements() {
      HeavyKeeper<String> hk = new HeavyKeeper<>(3, 8, 7, 0.9, HASH);

      hk.incrBy("c", 1);
      hk.incrBy("a", 10);
      hk.incrBy("b", 5);

      List<HeavyKeeper.KeyFrequency<String>> before = hk.list();

      // Increment "c" to overtake "b" but not "a".
      hk.incrBy("c", 7);

      List<HeavyKeeper.KeyFrequency<String>> after = hk.list();
      assertThat(after.get(0).key()).isEqualTo("a");
      assertThat(after.get(1).key()).isEqualTo("c");
      assertThat(after.get(2).key()).isEqualTo("b");
      assertThat(after.get(1).count()).isEqualTo(8);
   }

   @Test
   public void testKEqualsOne() {
      HeavyKeeper<String> hk = new HeavyKeeper<>(1, 8, 7, 0.9, HASH);

      hk.incrBy("first", 10);
      assertThat(hk.query("first")).isTrue();

      // A newcomer with a higher count takes over.
      String expelled = hk.incrBy("second", 50);
      assertThat(expelled).isEqualTo("first");
      assertThat(hk.query("second")).isTrue();
      assertThat(hk.query("first")).isFalse();
      assertThat(hk.list()).hasSize(1);
   }

   @Test
   public void testGradualAccumulationPromotesItem() {
      HeavyKeeper<String> hk = new HeavyKeeper<>(2, 8, 7, 0.9, HASH);

      hk.incrBy("established-1", 100);
      hk.incrBy("established-2", 50);

      // Gradually accumulate a challenger one hit at a time.
      Set<String> expelled = new HashSet<>();
      for (int i = 0; i < 200; i++) {
         String result = hk.add("challenger");
         if (result != null) {
            expelled.add(result);
         }
      }

      assertThat(hk.query("challenger")).isTrue();
      assertThat(expelled).containsExactly("established-2");
   }
}
