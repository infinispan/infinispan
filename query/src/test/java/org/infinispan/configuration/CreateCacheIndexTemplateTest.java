package org.infinispan.configuration;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.util.List;

import org.infinispan.Cache;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.helper.TestQueryHelperFactory;
import org.infinispan.query.test.Person;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "configuration.CreateCacheIndexTemplateTest")
public class CreateCacheIndexTemplateTest extends SingleCacheManagerTest {

   private final String indexDirectory = TestingUtil.tmpDirectory(getClass());

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      //builder.indexing().index(Index.ALL).autoConfig(true);
      builder.indexing().index(Index.ALL)
            .addIndexedEntity(Person.class)
            //.addProperty("default.directory_provider", "filesystem")
            .addProperty("default.directory_provider", "infinispan")
            .addProperty("default.indexBase", indexDirectory)
            .addProperty("lucene_version", "LUCENE_CURRENT");
      return TestCacheManagerFactory.createCacheManager(builder);
   }

   @AfterClass
   protected void destroy() {
      //todo can multiple caches use the same dir for indexes if the use same index names?
      //todo can multiple caches use the same infinispan clustered index manager (or with same backing caches) for indexes if the use same index names?
      Util.recursiveFileRemove(indexDirectory);
   }

   public void testCreateCache() throws Exception {
      Cache<String, Person> defaultCache = cacheManager.getCache();

      defaultCache.put("fluffy", new Person("White Rabbit", "eats carrots and grass", 1));
      defaultCache.put("shady", new Person("Grey Rabbit", "eats carrots and apples", 1));
      defaultCache.put("bear", new Person("Pooh", "eats berries", 5));
      assertEquals(3, defaultCache.size());

      List<Person> list1 = TestQueryHelperFactory.<Person>createCacheQuery(defaultCache, "name", "rabbit").list();
      assertEquals(2, list1.size());

      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.read(cacheManager.getDefaultCacheConfiguration());
      //builder.template(false); //TODO [anistor] this is false already. maybe it was meant to cover up an old bug with config templates?

      cacheManager.defineConfiguration("newCache", builder.build());

      Cache<Object, Object> newCache = cacheManager.getCache("newCache");
      assertTrue(newCache.getCacheConfiguration().indexing().index().isEnabled());

      newCache.put("rabid", new Person("Black Rabbit", "smokes grass", 1));
      assertEquals(1, newCache.size());

      List<Person> list2 = TestQueryHelperFactory.<Person>createCacheQuery(newCache, "name", "rabbit").list();
      assertEquals(1, list2.size());

      // TODO bug: defaultCache and newCache share their QueryInterceptor and the QueryInterceptor holds a ref to SearchIntegrator of defaultCache
   }
}
