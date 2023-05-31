package org.infinispan.multimap.impl;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.distribution.BaseDistFunctionalTest;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.multimap.api.embedded.EmbeddedMultimapCacheManagerFactory;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.data.Person;
import org.testng.annotations.Test;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.functional.FunctionalTestUtils.await;
import static org.infinispan.multimap.impl.MultimapTestUtils.ELAIA;
import static org.infinispan.multimap.impl.MultimapTestUtils.NAMES_KEY;
import static org.infinispan.multimap.impl.MultimapTestUtils.OIHANA;
import static org.infinispan.multimap.impl.MultimapTestUtils.RAMON;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;

@Test(groups = "functional", testName = "distribution.DistributedMultimapListCacheTest")
public class DistributedMultimapListCacheTest extends BaseDistFunctionalTest<String, Collection<Person>> {

   protected Map<Address, EmbeddedMultimapListCache<String, Person>> listCluster = new HashMap<>();
   protected boolean fromOwner;

   public DistributedMultimapListCacheTest fromOwner(boolean fromOwner) {
      this.fromOwner = fromOwner;
      return this;
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      super.createCacheManagers();

      for (EmbeddedCacheManager cacheManager : cacheManagers) {
         EmbeddedMultimapCacheManager multimapCacheManager = (EmbeddedMultimapCacheManager) EmbeddedMultimapCacheManagerFactory.from(cacheManager);
         listCluster.put(cacheManager.getAddress(), multimapCacheManager.getMultimapList(cacheName));
      }
   }

   @Override
   protected SerializationContextInitializer getSerializationContext() {
      return MultimapSCI.INSTANCE;
   }

   @Override
   protected String[] parameterNames() {
      return concat(super.parameterNames(), "fromOwner");
   }

   @Override
   protected Object[] parameterValues() {
      return concat(super.parameterValues(), fromOwner ? Boolean.TRUE : Boolean.FALSE);
   }

   @Override
   public Object[] factory() {
      return new Object[]{
            new DistributedMultimapListCacheTest().fromOwner(false).cacheMode(CacheMode.DIST_SYNC).transactional(false),
            new DistributedMultimapListCacheTest().fromOwner(true).cacheMode(CacheMode.DIST_SYNC).transactional(false),
      };
   }

   @Override
   protected void initAndTest() {
      for (EmbeddedMultimapListCache list : listCluster.values()) {
         assertThat(await(list.size(NAMES_KEY))).isEqualTo(0L);
      }
   }

   protected EmbeddedMultimapListCache<String, Person> getMultimapCacheMember() {
      return listCluster.
            values().stream().findFirst().orElseThrow(() -> new IllegalStateException("Cluster is empty"));
   }

   public void testOfferFirstAndLast() {
      initAndTest();
      EmbeddedMultimapListCache<String, Person> list = getMultimapCacheMember();
      await(list.offerFirst(NAMES_KEY, OIHANA));
      assertValuesAndOwnership(NAMES_KEY, OIHANA);

      await(list.offerLast(NAMES_KEY, ELAIA));
      assertValuesAndOwnership(NAMES_KEY, ELAIA);
   }

   public void testPollFirstAndLast() {
      initAndTest();
      EmbeddedMultimapListCache<String, Person> list = getMultimapCacheMember();
      await(
            list.offerLast(NAMES_KEY, OIHANA)
                  .thenCompose(r1 -> list.offerLast(NAMES_KEY, ELAIA))
                  .thenCompose(r1 -> list.offerLast(NAMES_KEY, OIHANA))
                  .thenCompose(r1 -> list.offerLast(NAMES_KEY, ELAIA))
                  .thenCompose(r1 -> list.size(NAMES_KEY))
                  .thenAccept(size -> assertThat(size).isEqualTo(4))

      );

      await(
            list.pollLast(NAMES_KEY, 2)
                  .thenAccept(r1 -> assertThat(r1).containsExactly(ELAIA, OIHANA))

      );
      await(
            list.pollFirst(NAMES_KEY, 1)
                  .thenAccept(r1 -> assertThat(r1).containsExactly(OIHANA))

      );

      await(
            list.size(NAMES_KEY)
                  .thenAccept(r1 -> assertThat(r1).isEqualTo(1))

      );
   }

   public void testSize() {
      initAndTest();
      EmbeddedMultimapListCache<String, Person> list = getMultimapCacheMember();
      await(
            list.offerFirst(NAMES_KEY, OIHANA)
                  .thenCompose(r1 -> list.offerFirst(NAMES_KEY, OIHANA))
                  .thenCompose(r1 -> list.offerFirst(NAMES_KEY, OIHANA))
                  .thenCompose(r1 -> list.offerFirst(NAMES_KEY, OIHANA))
                  .thenCompose(r1 -> list.size(NAMES_KEY))
                  .thenAccept(size -> assertThat(size).isEqualTo(4))

      );
   }

   public void testIndex() {
      initAndTest();
      EmbeddedMultimapListCache<String, Person> list = getMultimapCacheMember();
      await(
            list.offerLast(NAMES_KEY, OIHANA)
                  .thenCompose(r1 -> list.offerLast(NAMES_KEY, OIHANA))
                  .thenCompose(r1 -> list.offerLast(NAMES_KEY, ELAIA))
                  .thenCompose(r1 -> list.offerLast(NAMES_KEY, OIHANA))
                  .thenCompose(r1 -> list.index(NAMES_KEY, 2))
                  .thenAccept(v -> assertThat(v).isEqualTo(ELAIA))

      );
   }

   public void testSubList() {
      initAndTest();
      EmbeddedMultimapListCache<String, Person> list = getMultimapCacheMember();
      await(
            list.offerLast(NAMES_KEY, OIHANA)
                  .thenCompose(r1 -> list.offerLast(NAMES_KEY, OIHANA))
                  .thenCompose(r1 -> list.offerLast(NAMES_KEY, ELAIA))
                  .thenCompose(r1 -> list.offerLast(NAMES_KEY, OIHANA))
                  .thenCompose(r1 -> list.subList(NAMES_KEY, 1, 3))
                  .thenAccept(c -> assertThat(c).containsExactly(OIHANA, ELAIA, OIHANA))

      );
   }

   public void testSet() {
      initAndTest();
      EmbeddedMultimapListCache<String, Person> list = getMultimapCacheMember();
      await(
            list.offerLast(NAMES_KEY, OIHANA)
                  .thenCompose(r1 -> list.offerLast(NAMES_KEY, OIHANA))
                  .thenCompose(r1 -> list.offerLast(NAMES_KEY, ELAIA))
                  .thenCompose(r1 -> list.offerLast(NAMES_KEY, OIHANA))
                  .thenCompose(r1 -> list.index(NAMES_KEY, 2))
                  .thenAccept(v -> assertThat(v).isEqualTo(ELAIA))

      );

      await(
            list.set(NAMES_KEY, 2, OIHANA)
                  .thenCompose(r1 -> list.index(NAMES_KEY, 2))
                  .thenAccept(v -> assertThat(v).isEqualTo(OIHANA))

      );
   }

   public void testIndexOf() {
      initAndTest();
      EmbeddedMultimapListCache<String, Person> list = getMultimapCacheMember();
      await(
            list.offerLast(NAMES_KEY, OIHANA)
                  .thenCompose(r1 -> list.offerLast(NAMES_KEY, OIHANA))
                  .thenCompose(r1 -> list.offerLast(NAMES_KEY, ELAIA))
                  .thenCompose(r1 -> list.offerLast(NAMES_KEY, OIHANA))
                  .thenCompose(r1 -> list.indexOf(NAMES_KEY, OIHANA, 0L, null, null))
                  .thenAccept(v -> assertThat(v).containsExactly(0L, 1L, 3L))

      );
   }

   public void testInsert() {
      initAndTest();
      EmbeddedMultimapListCache<String, Person> list = getMultimapCacheMember();
      await(
            list.offerLast(NAMES_KEY, OIHANA)
                  .thenCompose(r1 -> list.insert(NAMES_KEY, true, OIHANA, ELAIA))
                  .thenAccept(v -> assertThat(v).isEqualTo(2))

      );
   }

   public void testRemove() {
      initAndTest();
      EmbeddedMultimapListCache<String, Person> list = getMultimapCacheMember();
      await(
            list.offerLast(NAMES_KEY, OIHANA)
                  .thenCompose(r1 -> list.remove(NAMES_KEY, 1, OIHANA))
                  .thenAccept(v -> assertThat(v).isEqualTo(1))

      );
   }

   public void testTrim() {
      initAndTest();
      EmbeddedMultimapListCache<String, Person> list = getMultimapCacheMember();
      await(
            list.offerLast(NAMES_KEY, OIHANA)
                  .thenCompose(r -> list.offerLast(NAMES_KEY, ELAIA))
                  .thenCompose(r -> list.offerLast(NAMES_KEY, RAMON))
                  .thenCompose(r -> list.trim(NAMES_KEY, 1, 2))
                  .thenCompose(r -> list.subList(NAMES_KEY, 0, -1))
                  .thenAccept(v -> assertThat(v).containsExactly(ELAIA, RAMON))
      );
   }

   public void testRotate() {
      initAndTest();
      EmbeddedMultimapListCache<String, Person> list = getMultimapCacheMember();
      await(
            list.offerLast(NAMES_KEY, OIHANA)
                  .thenCompose(r1 -> list.offerLast(NAMES_KEY, ELAIA))
                  .thenCompose(r1 -> list.offerLast(NAMES_KEY, RAMON))
      );

      await(
            list.rotate(NAMES_KEY, false)
                  .thenAccept(v -> assertThat(v).isEqualTo(RAMON))

      );
      await(
            list.subList(NAMES_KEY, 0, -1)
                  .thenAccept(v -> assertThat(v).containsExactly(RAMON, OIHANA, ELAIA))

      );
   }

   protected void assertValuesAndOwnership(String key, Person value) {
      assertOwnershipAndNonOwnership(key, l1CacheEnabled);
      assertOnAllCaches(key, value);
   }

   protected void assertOnAllCaches(Object key, Person value) {
      for (Map.Entry<Address, EmbeddedMultimapListCache<String, Person>> entry : listCluster.entrySet()) {
         await(entry.getValue().get((String) key).thenAccept(v -> {
                  assertNotNull(format("values on the key %s must be not null", key), v);
                  assertTrue(format("values on the key '%s' must contain '%s' on node '%s'", key, value, entry.getKey()),
                        v.contains(value));
               })

         );
      }
   }
}
