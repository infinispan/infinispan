package org.infinispan.query.config;

import static org.testng.AssertJUnit.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.List;
import java.util.Set;

import org.apache.lucene.queryparser.classic.ParseException;
import org.hibernate.search.indexes.spi.DirectoryBasedIndexManager;
import org.hibernate.search.indexes.spi.IndexManager;
import org.hibernate.search.spi.SearchIntegrator;
import org.hibernate.search.store.DirectoryProvider;
import org.hibernate.search.store.impl.RAMDirectoryProvider;
import org.infinispan.Cache;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.Search;
import org.infinispan.query.SearchManager;
import org.infinispan.query.helper.TestQueryHelperFactory;
import org.infinispan.query.test.Person;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * @author Sanne Grinovero &lt;sanne@infinispan.org&gt; (C) 2011 Red Hat Inc.
 */
@Test(groups = "unit", testName = "query.config.MultipleCachesTest")
public class MultipleCachesTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      String config = TestingUtil.wrapXMLWithSchema(
            "<cache-container default-cache=\"default\">" +
            "   <local-cache name=\"default\">\n" +
            "      <indexing index=\"NONE\" />\n" +
            "   </local-cache>\n" +
            "   <local-cache name=\"indexingenabled\">\n" +
            "      <indexing index=\"LOCAL\" >\n" +
            "            <property name=\"default.directory_provider\">local-heap</property>\n" +
            "            <property name=\"lucene_version\">LUCENE_CURRENT</property>\n" +
            "      </indexing>\n" +
            "   </local-cache>\n" +
            "</cache-container>"
      );
      log.tracef("Using test configuration:\n%s", config);
      InputStream is = new ByteArrayInputStream(config.getBytes());
      final EmbeddedCacheManager cm;
      try {
         cm = TestCacheManagerFactory.fromStream(is);
      }
      finally {
         is.close();
      }
      cache = cm.getCache();
      return cm;
   }

   @Test(expectedExceptions = IllegalStateException.class)
   public void queryNotIndexedCache() throws ParseException {
      cacheManager.defineConfiguration("notIndexedA", cacheManager.getDefaultCacheConfiguration());
      final Cache<Object, Object> notIndexedCache = cacheManager.getCache("notIndexedA");
      notIndexedCache.put("1", new Person("A Person's Name", "A paragraph containing some text", 75));
      CacheQuery<Person> cq = TestQueryHelperFactory.createCacheQuery(cache, "name", "Name");
      assertEquals(1, cq.getResultSize());
      List<Person> l =  cq.list();
      assertEquals(1, l.size());
      Person p = l.get(0);
      assertEquals("A Person's Name", p.getName());
      assertEquals("A paragraph containing some text", p.getBlurb());
      assertEquals(75, p.getAge());
   }

   @Test
   public void notIndexedCacheNormalUse() {
      cacheManager.defineConfiguration("notIndexedB", cacheManager.getDefaultCacheConfiguration());
      final Cache<Object, Object> notIndexedCache = cacheManager.getCache("notIndexedB");
      notIndexedCache.put("1", new Person("A Person's Name", "A paragraph containing some text", 75));
      assert notIndexedCache.get("1") != null;
   }

   @Test
   public void indexedCache() throws ParseException {
      Cache<Object, Object> indexedCache = cacheManager.getCache("indexingenabled");
      useQuery(indexedCache);
   }

   private void useQuery(Cache<Object, Object> indexedCache) throws ParseException {
      indexedCache.put("1", new Person("A Person's Name", "A paragraph containing some text", 75));
      CacheQuery<Person> cq = TestQueryHelperFactory.createCacheQuery(indexedCache, "name", "Name");
      assertEquals(1, cq.getResultSize());
      List<Person> l =  cq.list();
      assertEquals(1, l.size());
      Person p = l.get(0);
      assertEquals("A Person's Name", p.getName());
      assertEquals("A paragraph containing some text", p.getBlurb());
      assertEquals(75, p.getAge());

      SearchManager queryFactory = Search.getSearchManager(indexedCache);
      SearchIntegrator searchImpl = queryFactory.unwrap(SearchIntegrator.class);
      Set<IndexManager> indexManagers = searchImpl.getIndexBindings().get(Person.class).getIndexManagerSelector().all();
      assert indexManagers != null && indexManagers.size() == 1;
      DirectoryBasedIndexManager directory = (DirectoryBasedIndexManager)indexManagers.iterator().next();
      DirectoryProvider directoryProvider = directory.getDirectoryProvider();
      assert directoryProvider instanceof RAMDirectoryProvider : "configuration properties where ignored";
   }
}
