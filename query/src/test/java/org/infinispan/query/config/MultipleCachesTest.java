package org.infinispan.query.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.AssertJUnit.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.List;

import org.apache.lucene.store.ByteBuffersDirectory;
import org.infinispan.Cache;
import org.infinispan.commons.api.query.Query;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.objectfilter.ParsingException;
import org.infinispan.query.helper.IndexAccessor;
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
            "      <indexing enabled=\"false\" />\n" +
            "   </local-cache>\n" +
            "   <local-cache name=\"indexingenabled\">\n" +
            "      <indexing storage=\"local-heap\">\n" +
            "         <indexed-entities>\n" +
            "            <indexed-entity>org.infinispan.query.test.Person</indexed-entity>\n" +
            "         </indexed-entities>\n" +
            "      </indexing>\n" +
            "   </local-cache>\n" +
            "</cache-container>"
      );
      log.tracef("Using test configuration:\n%s", config);
      InputStream is = new ByteArrayInputStream(config.getBytes());
      final EmbeddedCacheManager cm;
      try {
         cm = TestCacheManagerFactory.fromStream(is);
      } finally {
         is.close();
      }
      cache = cm.getCache();
      return cm;
   }

   @Test(expectedExceptions = ParsingException.class)
   public void queryNotIndexedCache() {
      cacheManager.defineConfiguration("notIndexedA", cacheManager.getDefaultCacheConfiguration());
      final Cache<Object, Object> notIndexedCache = cacheManager.getCache("notIndexedA");
      notIndexedCache.put("1", new Person("A Person's Name", "A paragraph containing some text", 75));
      Query<Person> fullTextQuery = TestQueryHelperFactory.createCacheQuery(Person.class, cache, "name", "Name");
      assertEquals(1, fullTextQuery.execute().count().value());
      List<Person> l = fullTextQuery.execute().list();
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
   public void indexedCache() {
      Cache<Object, Object> indexedCache = cacheManager.getCache("indexingenabled");
      useQuery(indexedCache);
   }

   private void useQuery(Cache<Object, Object> indexedCache) {
      indexedCache.put("1", new Person("A Person's Name", "A paragraph containing some text", 75));
      Query<Person> cq = TestQueryHelperFactory.createCacheQuery(Person.class, indexedCache, "name", "Name");
      assertEquals(1, cq.execute().count().value());
      List<Person> l = cq.execute().list();
      assertEquals(1, l.size());
      Person p = l.get(0);
      assertEquals("A Person's Name", p.getName());
      assertEquals("A paragraph containing some text", p.getBlurb());
      assertEquals(75, p.getAge());

      IndexAccessor accessorTest = IndexAccessor.of(indexedCache, Person.class);
      assertThat(accessorTest.getIndexManager()).isNotNull();
      assertThat(accessorTest.getDirectory()).isInstanceOf(ByteBuffersDirectory.class);
   }
}
