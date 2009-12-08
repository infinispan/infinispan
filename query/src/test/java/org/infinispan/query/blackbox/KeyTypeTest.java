package org.infinispan.query.blackbox;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.TermQuery;
import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.manager.CacheManager;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.QueryFactory;
import org.infinispan.query.backend.QueryHelper;
import org.infinispan.query.test.Person;
import org.infinispan.query.test.CustomKey;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.lookup.DummyTransactionManagerLookup;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;
import org.testng.annotations.BeforeMethod;

import java.util.List;
import java.util.Properties;

/**
 * Class that will put in different kinds of keys into the cache and run a query on it to see if
 * different primitives will work as keys.
 *
 * @author Navin Surtani
 */

@Test(groups = "functional")
public class KeyTypeTest extends SingleCacheManagerTest{

   Cache<Object, Person> cache;
   QueryHelper qh;
   Person person1;

   @Override
   protected CacheManager createCacheManager() throws Exception {
      Configuration c = new Configuration();
      c.setTransactionManagerLookupClass(DummyTransactionManagerLookup.class.getName());
      return TestCacheManagerFactory.createCacheManager(c, true);
   }

   @BeforeMethod (alwaysRun = true)
   public void setUp() throws Exception{
      System.setProperty(QueryHelper.QUERY_ENABLED_PROPERTY, "true");
      System.setProperty(QueryHelper.QUERY_INDEX_LOCAL_ONLY_PROPERTY, "true");

      CacheManager manager = createCacheManager();
      cache = manager.getCache();
      qh = new QueryHelper(cache, new Properties(), Person.class);

      person1 = new Person();
      person1.setName("Navin");
      person1.setBlurb("Owns a macbook");
      person1.setAge(20);


   }

   @AfterMethod(alwaysRun = true)
   public void tearDown() {
      if (cache != null) {
         cache.clear();
         cache.stop();
      }
   }

   public void testPrimitiveAndStringKeys(){
      String key1 = "key1";
      int key2 = 2;
      byte key3 = (byte) 3;
      float key4 = (float) 4;
      long key5 = (long) 5;
      short key6 = (short) 6;
      boolean key7 = true;
      double key8 = (double) 8;
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
      Term term = new Term ("blurb", "owns");
      CacheQuery cacheQuery = new QueryFactory(cache, qh).getQuery(new TermQuery(term));
      assert cacheQuery.getResultSize() == 9;

      List<Object> found = cacheQuery.list();
      for (int i = 0; i < 9; i++){
         assert found.get(i).equals(person1);
      }

   }

   public void testCustomKeys(){
      CustomKey key1 = new CustomKey("Kim", 3);
      CustomKey key2 = new CustomKey("Jong", 4);
      CustomKey key3 = new CustomKey("Il", 2);

      cache.put(key1, person1);
      cache.put(key2, person1);
      cache.put(key3, person1);

      Term term = new Term("blurb", "owns");
      CacheQuery cacheQuery = new QueryFactory(cache, qh).getQuery(new TermQuery(term));
      assert cacheQuery.getResultSize() == 3;
   }
}
