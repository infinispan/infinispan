package org.infinispan.query.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.AssertJUnit.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.List;

import org.apache.lucene.store.ByteBuffersDirectory;
import org.hibernate.search.engine.backend.index.IndexManager;
import org.infinispan.commons.api.query.Query;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.helper.IndexAccessor;
import org.infinispan.query.helper.TestQueryHelperFactory;
import org.infinispan.query.test.Person;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(testName = "query.config.DeclarativeConfigTest", groups = "functional")
public class DeclarativeConfigTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      String config = TestingUtil.wrapXMLWithSchema(
            "<cache-container default-cache=\"default\">" +
            "   <local-cache name=\"default\">\n" +
            "      <indexing storage=\"local-heap\">\n" +
            "         <indexed-entities>\n" +
            "            <indexed-entity>org.infinispan.query.test.Person</indexed-entity>\n" +
            "         </indexed-entities>\n" +
            "      </indexing>\n" +
            "   </local-cache>\n" +
            "</cache-container>"
      );
      log.tracef("Using test configuration:\n%s", config);
      try (InputStream is = new ByteArrayInputStream(config.getBytes())) {
         cacheManager = TestCacheManagerFactory.fromStream(is);
      }
      cache = cacheManager.getCache();
      return cacheManager;
   }

   public void simpleIndexTest() {
      cache.put("1", new Person("A Person's Name", "A paragraph containing some text", 75));
      Query<Person> cq = TestQueryHelperFactory.createCacheQuery(Person.class, cache, "name", "Name");
      assertEquals(1, cq.execute().count().value());
      List<Person> l = cq.execute().list();
      assertEquals(1, l.size());
      Person p = l.get(0);
      assertEquals("A Person's Name", p.getName());
      assertEquals("A paragraph containing some text", p.getBlurb());
      assertEquals(75, p.getAge());
   }

   public void testPropertiesWhereRead() {
      IndexAccessor accessorTest = IndexAccessor.of(cache, Person.class);
      IndexManager indexManager = accessorTest.getIndexManager();
      assertThat(indexManager).isNotNull();
      assertThat(accessorTest.getDirectory()).isInstanceOf(ByteBuffersDirectory.class);
   }
}
