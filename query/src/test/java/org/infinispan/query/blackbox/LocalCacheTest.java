package org.infinispan.query.blackbox;

import org.infinispan.config.Configuration;
import org.infinispan.manager.CacheManager;
import org.infinispan.query.backend.QueryHelper;
import org.infinispan.query.helper.TestQueryHelperFactory;
import org.infinispan.query.test.Person;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.lookup.DummyTransactionManagerLookup;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * @author Navin Surtani
 */

@Test(groups = "functional")
public class LocalCacheTest extends AbstractLocalQueryTest {

   protected CacheManager createCacheManager() throws Exception {
      Configuration c = new Configuration();
      c.setTransactionManagerLookupClass(DummyTransactionManagerLookup.class.getName());
      return TestCacheManagerFactory.createCacheManager(c, true);
   }


   @BeforeMethod
   public void setUp() throws Exception {
      System.setProperty(QueryHelper.QUERY_ENABLED_PROPERTY, "true");
      System.setProperty(QueryHelper.QUERY_INDEX_LOCAL_ONLY_PROPERTY, "true");


      cache = createCacheManager().getCache();

      qh = TestQueryHelperFactory.createTestQueryHelperInstance(cache, Person.class);

      person1 = new Person();
      person1.setName("Navin Surtani");
      person1.setBlurb("Likes playing WoW");

      person2 = new Person();
      person2.setName("Big Goat");
      person2.setBlurb("Eats grass");

      person3 = new Person();
      person3.setName("Mini Goat");
      person3.setBlurb("Eats cheese");

      person5 = new Person();
      person5.setName("Smelly Cat");
      person5.setBlurb("Eats fish");

      //Put the 3 created objects in the cache.
      cache.put(key1, person1);
      cache.put(key2, person2);
      cache.put(key3, person3);
                  

   }
}
