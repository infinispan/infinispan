package org.infinispan.multimap.impl;

import org.infinispan.commons.CacheException;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.data.Person;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.infinispan.functional.FunctionalTestUtils.await;
import static org.infinispan.multimap.impl.EmbeddedMultimapListCache.ERR_ELEMENT_CAN_T_BE_NULL;
import static org.infinispan.multimap.impl.EmbeddedMultimapListCache.ERR_KEY_CAN_T_BE_NULL;
import static org.infinispan.multimap.impl.EmbeddedMultimapListCache.ERR_PIVOT_CAN_T_BE_NULL;
import static org.infinispan.multimap.impl.EmbeddedMultimapListCache.ERR_VALUE_CAN_T_BE_NULL;
import static org.infinispan.multimap.impl.MultimapTestUtils.ELAIA;
import static org.infinispan.multimap.impl.MultimapTestUtils.FELIX;
import static org.infinispan.multimap.impl.MultimapTestUtils.JULIEN;
import static org.infinispan.multimap.impl.MultimapTestUtils.KOLDO;
import static org.infinispan.multimap.impl.MultimapTestUtils.NAMES_KEY;
import static org.infinispan.multimap.impl.MultimapTestUtils.OIHANA;
import static org.infinispan.multimap.impl.MultimapTestUtils.PEPE;
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
      assertThatThrownBy(() ->
         await(listCache.offerFirst(null, OIHANA))
      ).isInstanceOf(NullPointerException.class)
            .hasMessageContaining(ERR_KEY_CAN_T_BE_NULL);

      assertThatThrownBy(() ->
         await(listCache.offerLast(null, OIHANA))
      ).isInstanceOf(NullPointerException.class)
            .hasMessageContaining(ERR_KEY_CAN_T_BE_NULL);

      assertThatThrownBy(() ->
         await(listCache.offerFirst(NAMES_KEY, null))
      ).isInstanceOf(NullPointerException.class)
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

   public void testSet() {
      await(listCache.offerLast(NAMES_KEY, OIHANA));
      await(listCache.offerLast(NAMES_KEY, ELAIA));
      await(listCache.offerLast(NAMES_KEY, KOLDO));
      await(listCache.offerLast(NAMES_KEY, FELIX));

      // head
      assertThat(await(listCache.index(NAMES_KEY, 0))).isEqualTo(OIHANA);
      assertThat(await(listCache.set(NAMES_KEY,0, RAMON))).isTrue();
      assertThat(await(listCache.index(NAMES_KEY, 0))).isEqualTo(RAMON);

      // tail
      assertThat(await(listCache.index(NAMES_KEY, -1))).isEqualTo(FELIX);
      assertThat(await(listCache.set(NAMES_KEY, -1, JULIEN))).isTrue();
      assertThat(await(listCache.index(NAMES_KEY, -1))).isEqualTo(JULIEN);

      // middle
      assertThat(await(listCache.index(NAMES_KEY, 1))).isEqualTo(ELAIA);
      assertThat(await(listCache.set(NAMES_KEY, 1, OIHANA))).isTrue();
      assertThat(await(listCache.index(NAMES_KEY, 1))).isEqualTo(OIHANA);

      assertThat(await(listCache.index(NAMES_KEY, -2))).isEqualTo(KOLDO);
      assertThat(await(listCache.set(NAMES_KEY, -2, ELAIA))).isTrue();
      assertThat(await(listCache.index(NAMES_KEY, -2))).isEqualTo(ELAIA);

      assertThat(await(listCache.index(NAMES_KEY, -3))).isEqualTo(OIHANA);
      assertThat(await(listCache.set(NAMES_KEY, 1, ELAIA))).isTrue();
      assertThat(await(listCache.index(NAMES_KEY, 1))).isEqualTo(ELAIA);

      // not existing
      assertThat(await(listCache.set("not_existing", 0, JULIEN))).isFalse();

      assertThatThrownBy(() -> {
         await(listCache.set(NAMES_KEY, 4, JULIEN));
      }).cause().cause()
            .isInstanceOf(CacheException.class)
            .cause().isInstanceOf(IndexOutOfBoundsException.class)
            .hasMessageContaining("Index is out of range");

      assertThatThrownBy(() -> {
         await(listCache.set(NAMES_KEY, -5, JULIEN));
      }).cause().cause()
            .isInstanceOf(CacheException.class)
            .cause().isInstanceOf(IndexOutOfBoundsException.class)
            .hasMessageContaining("Index is out of range");

      assertThatThrownBy(() -> {
         await(listCache.index(null, 10));
      }).isInstanceOf(NullPointerException.class)
            .hasMessageContaining(ERR_KEY_CAN_T_BE_NULL);

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

   public void testIndexOf() {
      await(listCache.offerLast(NAMES_KEY, OIHANA));//0
      await(listCache.offerLast(NAMES_KEY, ELAIA));//1
      await(listCache.offerLast(NAMES_KEY, KOLDO));//2
      await(listCache.offerLast(NAMES_KEY, RAMON));//3
      await(listCache.offerLast(NAMES_KEY, RAMON));//4
      await(listCache.offerLast(NAMES_KEY, JULIEN));//5
      await(listCache.offerLast(NAMES_KEY, ELAIA));//6
      await(listCache.offerLast(NAMES_KEY, OIHANA));//7
      await(listCache.offerLast(NAMES_KEY, OIHANA));//8
      await(listCache.offerLast(NAMES_KEY, OIHANA));//9

      // defaults
      assertThat(await(listCache.indexOf("non_existing", OIHANA, null, null, null))).isNull();
      assertThat(await(listCache.indexOf(NAMES_KEY, PEPE, null, null, null))).isEmpty();
      assertThat(await(listCache.indexOf(NAMES_KEY, OIHANA, null, null, null))).containsExactly(0L);
      assertThat(await(listCache.indexOf(NAMES_KEY, KOLDO, null, null, null))).containsExactly(2L);

      // count parameter
      assertThat(await(listCache.indexOf(NAMES_KEY, OIHANA, 1L, null, null))).containsExactly(0L);
      assertThat(await(listCache.indexOf(NAMES_KEY, OIHANA, 2L, null, null))).containsExactly(0L, 7L);
      assertThat(await(listCache.indexOf(NAMES_KEY, OIHANA, 0L, null, null))).containsExactly(0L, 7L, 8L, 9L);

      // rank parameter
      assertThat(await(listCache.indexOf(NAMES_KEY, OIHANA, null, 1L, null))).containsExactly(0L);
      assertThat(await(listCache.indexOf(NAMES_KEY, OIHANA, null, 2L, null))).containsExactly(7L);
      assertThat(await(listCache.indexOf(NAMES_KEY, OIHANA, null, -1L, null))).containsExactly(9L);
      assertThat(await(listCache.indexOf(NAMES_KEY, OIHANA, null, -2L, null))).containsExactly(8L);
      assertThat(await(listCache.indexOf(NAMES_KEY, KOLDO, null, 1L, null))).containsExactly(2L);
      assertThat(await(listCache.indexOf(NAMES_KEY, KOLDO, null, 2L, null))).isEmpty();

      // maxLen parameter
      assertThat(await(listCache.indexOf(NAMES_KEY, KOLDO, null, null, 1L))).isEmpty();
      assertThat(await(listCache.indexOf(NAMES_KEY, KOLDO, null, null, 3L))).containsExactly(2L);

      // count + rank
      assertThat(await(listCache.indexOf(NAMES_KEY, OIHANA, 2L, 2L, null))).containsExactly(7L, 8L);
      assertThat(await(listCache.indexOf(NAMES_KEY, OIHANA, 0L, 2L, null))).containsExactly(7L, 8L, 9L);
      assertThat(await(listCache.indexOf(NAMES_KEY, OIHANA, 0L, -2L, null))).containsExactly(8L, 7L, 0L);

      // count + maxlen
      assertThat(await(listCache.indexOf(NAMES_KEY, OIHANA, 0L, null, 3L))).containsExactly(0L);
      assertThat(await(listCache.indexOf(NAMES_KEY, OIHANA, 0L, null, 8L))).containsExactly(0L, 7L);

      // count + maxlen + rank
      assertThat(await(listCache.indexOf(NAMES_KEY, OIHANA, 0L, -1L, 3L))).containsExactly(9L, 8L, 7L);
      assertThat(await(listCache.indexOf(NAMES_KEY, OIHANA, 1L, -1L, 3L))).containsExactly(9L);
      assertThat(await(listCache.indexOf(NAMES_KEY, OIHANA, 2L, -2L, 3L))).containsExactly(8L, 7L);
      assertThat(await(listCache.indexOf(NAMES_KEY, OIHANA, 3L, -3L, 3L))).containsExactly(7L);
      assertThat(await(listCache.indexOf(NAMES_KEY, OIHANA, 1L, -2L, 1L))).isEmpty();
      assertThat(await(listCache.indexOf(NAMES_KEY, OIHANA, 1L, 2L, 1L))).isEmpty();

      assertThatThrownBy(() -> {
         await(listCache.indexOf(null, null, null, null, null));
      }).isInstanceOf(NullPointerException.class)
            .hasMessageContaining(ERR_KEY_CAN_T_BE_NULL);

      assertThatThrownBy(() -> {
         await(listCache.indexOf("foo", null, null, null, null));
      }).isInstanceOf(NullPointerException.class)
            .hasMessageContaining(ERR_ELEMENT_CAN_T_BE_NULL);

      assertThatThrownBy(() -> {
         await(listCache.indexOf(NAMES_KEY, ELAIA, -1L, null, null));
      }).isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("count can't be negative");

      assertThatThrownBy(() -> {
         await(listCache.indexOf(NAMES_KEY, ELAIA, null, 0L, null));
      }).isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("rank can't be zero");

      assertThatThrownBy(() -> {
         await(listCache.indexOf(NAMES_KEY, ELAIA, null, null, -1L));
      }).isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("maxLen can't be negative");

   }

   public void testInsertElement() {
      await(listCache.offerLast(NAMES_KEY, OIHANA));//0
      await(listCache.offerLast(NAMES_KEY, ELAIA));//1
      await(listCache.offerLast(NAMES_KEY, KOLDO));//2
      await(listCache.offerLast(NAMES_KEY, OIHANA));//3
      assertThat(await(listCache.insert("not_existing", false, OIHANA, ELAIA))).isEqualTo(0);
      assertThat(await(listCache.insert(NAMES_KEY, false, RAMON, ELAIA))).isEqualTo(-1);
      assertThat(await(listCache.insert(NAMES_KEY, false, OIHANA, RAMON))).isEqualTo(5);
      assertThat(await(listCache.subList(NAMES_KEY, 0, -1))).containsExactly(OIHANA, RAMON, ELAIA, KOLDO, OIHANA);
      assertThat(await(listCache.insert(NAMES_KEY, true, OIHANA, RAMON))).isEqualTo(6);
      assertThat(await(listCache.subList(NAMES_KEY, 0, -1))).containsExactly(RAMON, OIHANA, RAMON, ELAIA, KOLDO, OIHANA);
      assertThat(await(listCache.insert(NAMES_KEY, true, OIHANA, RAMON))).isEqualTo(7);
      assertThat(await(listCache.subList(NAMES_KEY, 0, -1))).containsExactly(RAMON, RAMON, OIHANA, RAMON, ELAIA, KOLDO, OIHANA);

      assertThatThrownBy(() -> await(listCache.insert(null, false, null, null)))
            .isInstanceOf(NullPointerException.class)
            .hasMessageContaining(ERR_KEY_CAN_T_BE_NULL);

      assertThatThrownBy(() -> await(listCache.insert(NAMES_KEY, false, null, null)))
            .isInstanceOf(NullPointerException.class)
            .hasMessageContaining(ERR_PIVOT_CAN_T_BE_NULL);

      assertThatThrownBy(() -> await(listCache.insert(NAMES_KEY, false, OIHANA, null)))
            .isInstanceOf(NullPointerException.class)
            .hasMessageContaining(ERR_ELEMENT_CAN_T_BE_NULL);
   }

   public void testRemoveElement() {
      await(listCache.offerLast(NAMES_KEY, OIHANA));
      await(listCache.offerLast(NAMES_KEY, ELAIA));
      await(listCache.offerLast(NAMES_KEY, ELAIA));
      await(listCache.offerLast(NAMES_KEY, ELAIA));
      await(listCache.offerLast(NAMES_KEY, KOLDO));
      await(listCache.offerLast(NAMES_KEY, KOLDO));
      await(listCache.offerLast(NAMES_KEY, OIHANA));
      assertThat(await(listCache.remove("not_existing", 0, OIHANA))).isEqualTo(0);
      assertThat(await(listCache.remove(NAMES_KEY, 0, RAMON))).isEqualTo(0);
      assertThat(await(listCache.remove(NAMES_KEY, 0, ELAIA))).isEqualTo(3);
      assertThat(await(listCache.subList(NAMES_KEY, 0, -1))).containsExactly(OIHANA, KOLDO, KOLDO, OIHANA);
      assertThat(await(listCache.remove(NAMES_KEY, 3, KOLDO))).isEqualTo(2);
      assertThat(await(listCache.subList(NAMES_KEY, 0, -1))).containsExactly(OIHANA, OIHANA);
      assertThat(await(listCache.remove(NAMES_KEY, 1, OIHANA))).isEqualTo(1);
      assertThat(await(listCache.subList(NAMES_KEY, 0, -1))).containsExactly(OIHANA);
      assertThat(await(listCache.remove(NAMES_KEY, 1, OIHANA))).isEqualTo(1);
      assertThat(await(listCache.containsKey(NAMES_KEY))).isFalse();

      assertThatThrownBy(() -> await(listCache.remove(null, 0, null)))
            .isInstanceOf(NullPointerException.class)
            .hasMessageContaining(ERR_KEY_CAN_T_BE_NULL);

      assertThatThrownBy(() -> await(listCache.remove(NAMES_KEY, 0, null)))
            .isInstanceOf(NullPointerException.class)
            .hasMessageContaining(ERR_ELEMENT_CAN_T_BE_NULL);
   }

   public void testTrim() {
      await(listCache.offerLast(NAMES_KEY, OIHANA));
      await(listCache.offerLast(NAMES_KEY, ELAIA));
      await(listCache.offerLast(NAMES_KEY, KOLDO));
      await(listCache.offerLast(NAMES_KEY, RAMON));
      await(listCache.offerLast(NAMES_KEY, JULIEN));

      assertThat(await(listCache.trim("not_existing", 0, 0))).isFalse();
      assertThat(await(listCache.trim(NAMES_KEY, 0, 0))).isTrue();
      assertThat(await(listCache.subList(NAMES_KEY, 0, -1))).containsExactly(OIHANA);
      await(listCache.offerLast(NAMES_KEY, ELAIA));
      await(listCache.offerLast(NAMES_KEY, KOLDO));
      await(listCache.offerLast(NAMES_KEY, RAMON));
      await(listCache.offerLast(NAMES_KEY, JULIEN));
      assertThat(await(listCache.trim(NAMES_KEY, -3, -3))).isTrue();
      assertThat(await(listCache.subList(NAMES_KEY, 0, -1))).containsExactly(KOLDO);
      await(listCache.offerFirst(NAMES_KEY, ELAIA));
      await(listCache.offerFirst(NAMES_KEY, OIHANA));
      await(listCache.offerLast(NAMES_KEY, RAMON));
      await(listCache.offerLast(NAMES_KEY, JULIEN));
      assertThat(await(listCache.trim(NAMES_KEY, 0, 2))).isTrue();
      assertThat(await(listCache.subList(NAMES_KEY, 0, -1))).containsExactly(OIHANA, ELAIA, KOLDO);
      await(listCache.offerLast(NAMES_KEY, RAMON));
      await(listCache.offerLast(NAMES_KEY, JULIEN));
      assertThat(await(listCache.trim(NAMES_KEY, -1, -3))).isTrue();
      assertThat(await(listCache.subList(NAMES_KEY, 0, -1))).isNull();
      await(listCache.offerLast(NAMES_KEY, ELAIA));
      await(listCache.offerLast(NAMES_KEY, KOLDO));
      await(listCache.offerLast(NAMES_KEY, RAMON));
      await(listCache.offerLast(NAMES_KEY, JULIEN));
      assertThat(await(listCache.trim(NAMES_KEY, 3, 2))).isTrue();
      assertThat(await(listCache.subList(NAMES_KEY, 0, -1))).isNull();
      await(listCache.offerLast(NAMES_KEY, OIHANA));
      await(listCache.offerLast(NAMES_KEY, ELAIA));
      await(listCache.offerLast(NAMES_KEY, KOLDO));
      await(listCache.offerLast(NAMES_KEY, RAMON));
      await(listCache.offerLast(NAMES_KEY, JULIEN));
      assertThat(await(listCache.trim(NAMES_KEY, 1, -2))).isTrue();
      assertThat(await(listCache.subList(NAMES_KEY, 0, -1))).containsExactly(ELAIA, KOLDO, RAMON);

      assertThatThrownBy(() -> await(listCache.subList(null, 1, 10)))
            .isInstanceOf(NullPointerException.class)
            .hasMessageContaining(ERR_KEY_CAN_T_BE_NULL);
   }

   public void testRotate() {
      await(listCache.offerLast(NAMES_KEY, OIHANA));
      await(listCache.offerLast(NAMES_KEY, ELAIA));
      await(listCache.offerLast(NAMES_KEY, KOLDO));
      await(listCache.offerLast(NAMES_KEY, RAMON));
      await(listCache.offerLast(NAMES_KEY, JULIEN));

      assertThat(await(listCache.rotate("not_existing", true))).isNull();
      assertThat(await(listCache.subList(NAMES_KEY, 0, -1))).containsExactly(OIHANA, ELAIA, KOLDO, RAMON, JULIEN);
      assertThat(await(listCache.rotate(NAMES_KEY, true))).isEqualTo(OIHANA);
      assertThat(await(listCache.subList(NAMES_KEY, 0, -1))).containsExactly(ELAIA, KOLDO, RAMON, JULIEN, OIHANA);
      assertThat(await(listCache.rotate(NAMES_KEY, false))).isEqualTo(OIHANA);
      assertThat(await(listCache.subList(NAMES_KEY, 0, -1))).containsExactly(OIHANA, ELAIA, KOLDO, RAMON, JULIEN);

      assertThatThrownBy(() -> await(listCache.rotate(null, true)))
            .isInstanceOf(NullPointerException.class)
            .hasMessageContaining(ERR_KEY_CAN_T_BE_NULL);
   }
}
