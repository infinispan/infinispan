package org.infinispan.multimap.impl;

import jakarta.transaction.TransactionManager;
import org.infinispan.multimap.api.embedded.MultimapCache;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.data.Person;

import java.util.Map;

import static java.lang.String.format;
import static org.infinispan.functional.FunctionalTestUtils.await;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

public class MultimapTestUtils {
   public static final String NAMES_KEY = "names";
   public static final String EMPTY_KEY = "";
   public static final String NULL_KEY = null;
   public static final Person JULIEN = new Person("Julien");
   public static final Person OIHANA = new Person("Oihana");
   public static final Person RAMON = new Person("Ramon");
   public static final Person KOLDO = new Person("Koldo");
   public static final Person ELAIA = new Person("Elaia");
   public static final Person FELIX = new Person("Felix");
   public static final Person IGOR = new Person("Igor");
   public static final Person IZARO = new Person("Izaro");
   public static final SuperPerson PEPE = new SuperPerson("Pepe");
   public static final SuperPerson NULL_USER = null;

   public static TransactionManager getTransactionManager(MultimapCache multimapCache) {
      EmbeddedMultimapCache embeddedMultimapCache = (EmbeddedMultimapCache) multimapCache;
      return embeddedMultimapCache == null ? null : embeddedMultimapCache.getCache().getAdvancedCache().getTransactionManager();
   }

   public static void putValuesOnMultimapCache(MultimapCache<String, Person> multimapCache, String key, Person... values) {
      for (int i = 0; i < values.length; i++) {
         await(multimapCache.put(key, values[i]));
      }
   }

   public static void putValuesOnMultimapCache(Map<Address, MultimapCache<String, Person>> cluster, String key, Person... values) {
      for (MultimapCache mc : cluster.values()) {
         putValuesOnMultimapCache(mc, key, values);
      }
   }

   public static void assertMultimapCacheSize(MultimapCache<String, Person> multimapCache, int expectedSize) {
      assertEquals(expectedSize, await(multimapCache.size()).intValue());
   }

   public static void assertMultimapCacheSize(Map<Address, MultimapCache<String, Person>> cluster, int expectedSize) {
      for (MultimapCache mc : cluster.values()) {
         assertMultimapCacheSize(mc, expectedSize);
      }
   }

   public static void assertContaisKeyValue(MultimapCache<String, Person> multimapCache, String key, Person value) {
      Address address = ((EmbeddedMultimapCache) multimapCache).getCache().getCacheManager().getAddress();
      await(multimapCache.get(key).thenAccept(v -> {
         assertTrue(format("get method call : multimap '%s' must contain key '%s' value '%s' pair", address, key, value), v.contains(value));
      }));
      await(multimapCache.containsEntry(key, value).thenAccept(v -> {
         assertTrue(format("containsEntry method call : multimap '%s' must contain key '%s' value '%s' pair", address, key, value), v);
      }));
   }

}
