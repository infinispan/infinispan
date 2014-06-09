package org.infinispan.query.config;

import static junit.framework.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.List;

import org.apache.lucene.queryparser.classic.ParseException;
import org.hibernate.search.engine.spi.SearchFactoryImplementor;
import org.hibernate.search.indexes.impl.DirectoryBasedIndexManager;
import org.hibernate.search.indexes.spi.IndexManager;
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
 * @author Sanne Grinovero <sanne@infinispan.org> (C) 2011 Red Hat Inc.
 */
@Test(groups = "unit", testName = "query.config.MultipleCachesTest")
public class MultipleCachesTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      String config = TestingUtil.INFINISPAN_START_TAG + "\n" +
            "<cache-container default-cache=\"default\">" +
            "   <local-cache name=\"default\">\n" +
            "      <indexing index=\"NONE\" />\n" +
            "   </local-cache>\n" +
            "   <local-cache name=\"indexingenabled\">\n" +
            "      <indexing index=\"LOCAL\" >\n" +
            "            <property name=\"default.directory_provider\">ram</property>\n" +
            "            <property name=\"lucene_version\">LUCENE_CURRENT</property>\n" +
            "      </indexing>\n" +
            "   </local-cache>\n" +
            "</cache-container>"
            + TestingUtil.INFINISPAN_END_TAG;
      System.out.println("Using test configuration:\n\n" + config + "\n");
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

   @Test(expectedExceptions = IllegalArgumentException.class)
   public void queryNotIndexedCache() throws ParseException {
      final Cache<Object, Object> notIndexedCache = cacheManager.getCache("notIndexedA");
      notIndexedCache.put("1", new Person("A Person's Name", "A paragraph containing some text", 75));
      CacheQuery cq = TestQueryHelperFactory.createCacheQuery(cache, "name", "Name");
      assertEquals(1, cq.getResultSize());
      List<Object> l =  cq.list();
      assertEquals(1, l.size());
      Person p = (Person) l.get(0);
      assertEquals("A Person's Name", p.getName());
      assertEquals("A paragraph containing some text", p.getBlurb());
      assertEquals(75, p.getAge());
   }

   @Test
   public void notIndexedCacheNormalUse() {
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
      CacheQuery cq = TestQueryHelperFactory.createCacheQuery(indexedCache, "name", "Name");
      assertEquals(1, cq.getResultSize());
      List<Object> l =  cq.list();
      assertEquals(1, l.size());
      Person p = (Person) l.get(0);
      assertEquals("A Person's Name", p.getName());
      assertEquals("A paragraph containing some text", p.getBlurb());
      assertEquals(75, p.getAge());

      SearchManager queryFactory = Search.getSearchManager(indexedCache);
      SearchFactoryImplementor searchImpl = (SearchFactoryImplementor) queryFactory.getSearchFactory();
      IndexManager[] indexManagers = searchImpl.getIndexBinding(Person.class).getIndexManagers();
      assert indexManagers != null && indexManagers.length == 1;
      DirectoryBasedIndexManager directory = (DirectoryBasedIndexManager)indexManagers[0];
      DirectoryProvider directoryProvider = directory.getDirectoryProvider();
      assert directoryProvider instanceof RAMDirectoryProvider : "configuration properties where ignored";
   }
}
