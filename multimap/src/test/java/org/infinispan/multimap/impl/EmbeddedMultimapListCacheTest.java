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
import static org.infinispan.multimap.impl.MultimapTestUtils.KOLDO;
import static org.infinispan.multimap.impl.MultimapTestUtils.NAMES_KEY;
import static org.infinispan.multimap.impl.MultimapTestUtils.OIHANA;
import static org.infinispan.multimap.impl.MultimapTestUtils.RAMON;

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

   public void testIndex() {
      await(
            listCache.index(NAMES_KEY, 0)
                  .thenAccept(opt -> assertThat(opt).isNull())
      );

      await(
            listCache.index("not_exists", 4)
                  .thenAccept(opt -> assertThat(opt).isNull())
      );

      await(
            listCache.offerLast(NAMES_KEY, KOLDO)
                  .thenCompose(r3 -> listCache.index(NAMES_KEY, 0))
                  .thenCompose(v -> {
                     assertThat(v).isEqualTo(KOLDO);
                     return listCache.index(NAMES_KEY, -1);
                  })
                  .thenCompose(v -> {
                     assertThat(v).isEqualTo(KOLDO);
                     return listCache.index(NAMES_KEY, 1);
                  })
                  .thenCompose(v -> {
                     assertThat(v).isNull();
                     return listCache.index(NAMES_KEY, -2);
                  })
                  .thenAccept(opt -> assertThat(opt).isNull())
      );

      await(
            listCache.offerLast(NAMES_KEY, OIHANA)
                  .thenCompose(r2 -> listCache.offerLast(NAMES_KEY, ELAIA))
                  .thenCompose(r2 -> listCache.offerLast(NAMES_KEY, RAMON))
                  .thenCompose(r3 -> listCache.index(NAMES_KEY, 0))
                  .thenCompose(v -> {
                     assertThat(v).isEqualTo(KOLDO);
                     return listCache.index(NAMES_KEY, -1);
                  })
                  .thenCompose(v -> {
                     assertThat(v).isEqualTo(RAMON);
                     return listCache.index(NAMES_KEY, 3);
                  })
                  .thenCompose(v -> {
                     assertThat(v).isEqualTo(RAMON);
                     return listCache.index(NAMES_KEY, -2);
                  })
                  .thenCompose(v -> {
                     assertThat(v).isEqualTo(ELAIA);
                     return listCache.index(NAMES_KEY, 1);
                  })
                  .thenCompose(v -> {
                     assertThat(v).isEqualTo(OIHANA);
                     return listCache.index(NAMES_KEY, 10);
                  })
                  .thenAccept(opt -> assertThat(opt).isNull())
      );

      assertThatThrownBy(() -> {
         await(listCache.index(null, 10));
      }).isInstanceOf(NullPointerException.class)
            .hasMessageContaining(ERR_KEY_CAN_T_BE_NULL);

   }

   public void testPollFirst() {
      await(
            listCache.pollFirst(NAMES_KEY, 0)
                  .thenAccept(r1 -> assertThat(r1).isNull())
      );

      await(listCache.offerFirst(NAMES_KEY, OIHANA));

      await(
            listCache.pollFirst(NAMES_KEY, 0)
                  .thenAccept(r1 -> assertThat(r1).isEmpty())
      );

      await(
            listCache.pollFirst(NAMES_KEY, 1)
                  .thenAccept(r1 -> assertThat(r1).containsExactly(OIHANA))
      );
      await(
            listCache.pollFirst(NAMES_KEY, 1)
                  .thenAccept(r1 -> assertThat(r1).isNull())
      );

      // [OIHANA, ELAIA, KOLDO]
      await(listCache.offerLast(NAMES_KEY, OIHANA));
      await(listCache.offerLast(NAMES_KEY, ELAIA));
      await(listCache.offerLast(NAMES_KEY, KOLDO));

      await(
            listCache.pollFirst(NAMES_KEY, 2)
                  .thenAccept(r1 -> assertThat(r1).containsExactly(OIHANA, ELAIA))
      );

      await(
            listCache.pollLast(NAMES_KEY, 1)
                  .thenAccept(r1 -> assertThat(r1).containsExactly(KOLDO))
      );

      await(
            listCache.pollFirst(NAMES_KEY, 1)
                  .thenAccept(r1 -> assertThat(r1).isNull())
      );

      // [OIHANA, ELAIA, KOLDO]
      await(listCache.offerLast(NAMES_KEY, OIHANA));
      await(listCache.offerLast(NAMES_KEY, ELAIA));
      await(listCache.offerLast(NAMES_KEY, KOLDO));
      await(
            listCache.pollFirst(NAMES_KEY, 4)
                  .thenAccept(r1 -> assertThat(r1).containsExactly(OIHANA, ELAIA, KOLDO))
      );
      await(
            listCache.pollLast(NAMES_KEY, 1)
                  .thenAccept(r1 -> assertThat(r1).isNull())
      );
   }

   public void testPollLast() {
      await(
            listCache.pollLast(NAMES_KEY, 0)
                  .thenAccept(r1 -> assertThat(r1).isNull())
      );

      await(listCache.offerFirst(NAMES_KEY, OIHANA));

      await(
            listCache.pollLast(NAMES_KEY, 0)
                  .thenAccept(r1 -> assertThat(r1).isEmpty())
      );

      await(
            listCache.pollLast(NAMES_KEY, 1)
                  .thenAccept(r1 -> assertThat(r1).containsExactly(OIHANA))
      );
      await(
            listCache.pollLast(NAMES_KEY, 1)
                  .thenAccept(r1 -> assertThat(r1).isNull())
      );

      // [OIHANA, ELAIA, KOLDO]
      await(listCache.offerLast(NAMES_KEY, OIHANA));
      await(listCache.offerLast(NAMES_KEY, ELAIA));
      await(listCache.offerLast(NAMES_KEY, KOLDO));

      await(
            listCache.pollLast(NAMES_KEY, 2)
                  .thenAccept(r1 -> assertThat(r1).containsExactly(KOLDO, ELAIA))
      );

      await(
            listCache.pollLast(NAMES_KEY, 1)
                  .thenAccept(r1 -> assertThat(r1).containsExactly(OIHANA))
      );

      await(
            listCache.pollLast(NAMES_KEY, 1)
                  .thenAccept(r1 -> assertThat(r1).isNull())
      );

      // [OIHANA, ELAIA, KOLDO]
      await(listCache.offerLast(NAMES_KEY, OIHANA));
      await(listCache.offerLast(NAMES_KEY, ELAIA));
      await(listCache.offerLast(NAMES_KEY, KOLDO));
      await(
            listCache.pollLast(NAMES_KEY, 4)
                  .thenAccept(r1 -> assertThat(r1).containsExactly(KOLDO, ELAIA, OIHANA))
      );
      await(
            listCache.pollLast(NAMES_KEY, 1)
                  .thenAccept(r1 -> assertThat(r1).isNull())
      );
   }

   public void testSubList() {
      await(listCache.offerLast(NAMES_KEY, OIHANA));
      await(listCache.offerLast(NAMES_KEY, ELAIA));
      await(listCache.offerLast(NAMES_KEY, KOLDO));
      await(listCache.offerLast(NAMES_KEY, RAMON));
      await(listCache.offerLast(NAMES_KEY, JULIEN));

      assertThat(await(listCache.subList(NAMES_KEY, 0, 0))).containsExactly(OIHANA);
      assertThat(await(listCache.subList(NAMES_KEY, 4, 4))).containsExactly(JULIEN);
      assertThat(await(listCache.subList(NAMES_KEY, 5, 5))).isEmpty();
      assertThat(await(listCache.subList(NAMES_KEY, -1, -1))).containsExactly(JULIEN);
      assertThat(await(listCache.subList(NAMES_KEY, -5, -5))).containsExactly(OIHANA);
      assertThat(await(listCache.subList(NAMES_KEY, -6, -6))).isEmpty();

      assertThat(await(listCache.subList(NAMES_KEY, 0, 4))).containsExactly(OIHANA, ELAIA, KOLDO, RAMON, JULIEN);
      assertThat(await(listCache.subList(NAMES_KEY, 0, 5))).containsExactly(OIHANA, ELAIA, KOLDO, RAMON, JULIEN);
      assertThat(await(listCache.subList(NAMES_KEY, 0, 3))).containsExactly(OIHANA, ELAIA, KOLDO, RAMON);
      assertThat(await(listCache.subList(NAMES_KEY, 1, 2))).containsExactly(ELAIA, KOLDO);

      assertThat(await(listCache.subList(NAMES_KEY, 1, 0))).isEmpty();
      assertThat(await(listCache.subList(NAMES_KEY, -1, 0))).isEmpty();
      assertThat(await(listCache.subList(NAMES_KEY, -1, -2))).isEmpty();

      assertThat(await(listCache.subList(NAMES_KEY, -5, -1))).containsExactly(OIHANA, ELAIA, KOLDO, RAMON, JULIEN);
      assertThat(await(listCache.subList(NAMES_KEY, -5, -2))).containsExactly(OIHANA, ELAIA, KOLDO, RAMON);
      assertThat(await(listCache.subList(NAMES_KEY, -4, -2))).containsExactly(ELAIA, KOLDO, RAMON);

      assertThat(await(listCache.subList(NAMES_KEY, 1, -1))).containsExactly(ELAIA, KOLDO, RAMON, JULIEN);
      assertThat(await(listCache.subList(NAMES_KEY, 1, -1))).containsExactly(ELAIA, KOLDO, RAMON, JULIEN);
      assertThat(await(listCache.subList(NAMES_KEY, -5, 1))).containsExactly(OIHANA, ELAIA);
      assertThat(await(listCache.subList(NAMES_KEY, -2, 4))).containsExactly(RAMON, JULIEN);
      assertThat(await(listCache.subList(NAMES_KEY, 0, -1))).containsExactly(OIHANA, ELAIA, KOLDO, RAMON, JULIEN);
      assertThat(await(listCache.subList(NAMES_KEY, 2, -2))).containsExactly(KOLDO, RAMON);
      assertThat(await(listCache.subList(NAMES_KEY, -1, 7))).containsExactly(JULIEN);
      assertThat(await(listCache.subList(NAMES_KEY, 2, -6))).isEmpty();

      assertThatThrownBy(() -> {
         await(listCache.subList(null, 1, 10));
      }).isInstanceOf(NullPointerException.class)
            .hasMessageContaining(ERR_KEY_CAN_T_BE_NULL);

      assertThat(await(listCache.subList("not_existing", 1, 10))).isNull();
   }
}
