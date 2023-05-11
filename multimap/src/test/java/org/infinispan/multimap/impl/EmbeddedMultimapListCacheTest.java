package org.infinispan.multimap.impl;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.data.Person;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.infinispan.functional.FunctionalTestUtils.await;
import static org.infinispan.multimap.impl.EmbeddedMultimapListCache.ERR_KEY_CAN_T_BE_NULL;
import static org.infinispan.multimap.impl.EmbeddedMultimapListCache.ERR_VALUE_CAN_T_BE_NULL;
import static org.infinispan.multimap.impl.MultimapTestUtils.ELAIA;
import static org.infinispan.multimap.impl.MultimapTestUtils.JULIEN;
import static org.infinispan.multimap.impl.MultimapTestUtils.NAMES_KEY;
import static org.infinispan.multimap.impl.MultimapTestUtils.OIHANA;

/**
 * Single Multimap Cache Test with Linked List
 *
 * @since 15.0
 */
@Test(groups = "functional", testName = "multimap.EmbeddedMultimapListCacheTest")
public class EmbeddedMultimapListCacheTest extends SingleCacheManagerTest {

   EmbeddedMultimapListCache<String, Person> listCache;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      // start a single cache instance
      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(MultimapSCI.INSTANCE);
      ConfigurationBuilder builder = new ConfigurationBuilder();
      cm.createCache("test", builder.build());
      cm.getClassAllowList().addClasses(Person.class);
      listCache = new EmbeddedMultimapListCache<>(cm.getCache("test"));
      return cm;
   }

   public void testOfferLast() {
      await(
            listCache.offerLast(NAMES_KEY, JULIEN)
                  .thenCompose(r1 -> listCache.offerLast(NAMES_KEY, OIHANA))
                  .thenCompose(r2 -> listCache.offerLast(NAMES_KEY, ELAIA))
                  .thenCompose(r3 -> listCache.get(NAMES_KEY))
                  .thenAccept(v -> {
                           assertThat(v).containsExactly(JULIEN, OIHANA, ELAIA);
                        }
                  )
      );
   }

   public void testOfferFirst() {
      await(
            listCache.offerFirst(NAMES_KEY, JULIEN)
                  .thenCompose(r1 -> listCache.offerFirst(NAMES_KEY, OIHANA))
                  .thenCompose(r2 -> listCache.offerFirst(NAMES_KEY, ELAIA))
                  .thenCompose(r3 -> listCache.get(NAMES_KEY))
                  .thenAccept(v ->
                           assertThat(v).containsExactly(ELAIA, OIHANA, JULIEN)
                  )
      );
   }

   public void testOfferWithDuplicates() {
      await(
            listCache.offerLast(NAMES_KEY, OIHANA)
                  .thenCompose(r1 -> listCache.offerFirst(NAMES_KEY, OIHANA))
                  .thenCompose(r2 -> listCache.offerLast(NAMES_KEY, ELAIA))
                  .thenCompose(r2 -> listCache.offerFirst(NAMES_KEY, ELAIA))
                  .thenCompose(r2 -> listCache.offerLast(NAMES_KEY, OIHANA))
                  .thenCompose(r3 -> listCache.get(NAMES_KEY))
                  .thenAccept(v ->
                           assertThat(v).containsExactly(ELAIA, OIHANA, OIHANA, ELAIA, OIHANA)
                  )
      );
   }

   public void testOfferWithNullArguments() {
      assertThatThrownBy(() -> {
         await(listCache.offerFirst(null, OIHANA));
      }).isInstanceOf(NullPointerException.class)
            .hasMessageContaining(ERR_KEY_CAN_T_BE_NULL);

      assertThatThrownBy(() -> {
         await(listCache.offerLast(null, OIHANA));
      }).isInstanceOf(NullPointerException.class)
            .hasMessageContaining(ERR_KEY_CAN_T_BE_NULL);

      assertThatThrownBy(() -> {
         await(listCache.offerFirst(NAMES_KEY, null));
      }).isInstanceOf(NullPointerException.class)
            .hasMessageContaining(ERR_VALUE_CAN_T_BE_NULL);

      assertThatThrownBy(() -> {
         await(listCache.offerLast(NAMES_KEY, null));
      }).isInstanceOf(NullPointerException.class)
            .hasMessageContaining(ERR_VALUE_CAN_T_BE_NULL);

   }

   public void testSize() {
      await(
            listCache.size(NAMES_KEY)
                  .thenAccept(size -> assertThat(size).isZero())
      );

      await(
            listCache.offerFirst(NAMES_KEY, OIHANA)
                  .thenCompose(r1 -> listCache.size(NAMES_KEY))
                  .thenAccept(size -> assertThat(size).isEqualTo(1))

      );

      await(
            listCache.size("not_exists")
                  .thenAccept(size -> assertThat(size).isZero())
      );

      assertThatThrownBy(() -> {
         await(listCache.size(null));
      }).isInstanceOf(NullPointerException.class)
            .hasMessageContaining(ERR_KEY_CAN_T_BE_NULL);
   }
}
