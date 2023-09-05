package org.infinispan.query.blackbox;

import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;
import static org.testng.AssertJUnit.assertEquals;

import java.util.List;

import org.infinispan.commons.api.query.Query;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.test.CustomKey;
import org.infinispan.query.test.Person;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.Test;

/**
 * Class that will put in different kinds of keys into the cache and run a query on it to see if
 * different primitives will work as keys.
 *
 * @author Navin Surtani
 */
@Test(groups = "functional", testName = "query.blackbox.KeyTypeTest")
public class KeyTypeTest extends SingleCacheManagerTest {

   private Person person1;

   public KeyTypeTest() {
      cleanup = CleanupPhase.AFTER_METHOD;
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder cfg = getDefaultStandaloneCacheConfig(true);
      cfg
            .transaction()
            .transactionMode(TransactionMode.TRANSACTIONAL)
            .indexing().enable()
            .storage(LOCAL_HEAP)
            .addIndexedEntity(Person.class);
      cacheManager = TestCacheManagerFactory.createCacheManager(cfg);

      person1 = new Person();
      person1.setName("Navin");
      person1.setBlurb("Owns a macbook");
      person1.setAge(20);
      return cacheManager;
   }

   public void testPrimitiveAndStringKeys() {
      String key1 = "key1";
      int key2 = 2;
      byte key3 = 3;
      float key4 = 4;
      long key5 = 5;
      short key6 = 6;
      boolean key7 = true;
      double key8 = 8;
      char key9 = '9';

      cache.put(key1, person1);
      cache.put(key2, person1);
      cache.put(key3, person1);
      cache.put(key4, person1);
      cache.put(key5, person1);
      cache.put(key6, person1);
      cache.put(key7, person1);
      cache.put(key8, person1);
      cache.put(key9, person1);

      // Going to search the 'blurb' field for 'owns'
      Query cacheQuery = cache.query(getQuery());
      assertEquals(9, cacheQuery.execute().count().value());

      List<Person> found = cacheQuery.list();
      for (int i = 0; i < 9; i++) {
         assertEquals(person1, found.get(i));
      }
   }

   private String getQuery() {
      return String.format("FROM %s WHERE blurb:'owns'", Person.class.getName());
   }

   public void testCustomKeys() {
      CustomKey key1 = new CustomKey(1, 2, 3);
      CustomKey key2 = new CustomKey(900, 800, 700);
      CustomKey key3 = new CustomKey(1024, 2048, 4096);

      cache.put(key1, person1);
      cache.put(key2, person1);
      cache.put(key3, person1);

      Query cacheQuery = cache.query(getQuery());
      assertEquals(3, cacheQuery.execute().count().value());
   }
}
