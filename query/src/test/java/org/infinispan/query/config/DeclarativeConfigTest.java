package org.infinispan.query.config;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.List;
import java.util.Set;

import org.hibernate.search.engine.spi.EntityIndexBinding;
import org.hibernate.search.indexes.spi.DirectoryBasedIndexManager;
import org.hibernate.search.indexes.spi.IndexManager;
import org.hibernate.search.spi.SearchIntegrator;
import org.hibernate.search.store.impl.RAMDirectoryProvider;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.CacheQuery;
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
            "      <indexing index=\"PRIMARY_OWNER\">\n" +
            "         <indexed-entities>\n" +
            "            <indexed-entity>org.infinispan.query.test.Person</indexed-entity>\n" +
            "         </indexed-entities>\n" +
            "         <property name=\"default.directory_provider\">local-heap</property>\n" +
            "         <property name=\"lucene_version\">LUCENE_CURRENT</property>\n" +
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

   public void simpleIndexTest() throws Exception {
      cache.put("1", new Person("A Person's Name", "A paragraph containing some text", 75));
      CacheQuery<Person> cq = TestQueryHelperFactory.createCacheQuery(cache, "name", "Name");
      assertEquals(1, cq.getResultSize());
      List<Person> l = cq.list();
      assertEquals(1, l.size());
      Person p = l.get(0);
      assertEquals("A Person's Name", p.getName());
      assertEquals("A paragraph containing some text", p.getBlurb());
      assertEquals(75, p.getAge());
   }

   public void testPropertiesWhereRead() {
      SearchIntegrator searchFactory = TestQueryHelperFactory.extractSearchFactory(cache);
      EntityIndexBinding indexBindingForEntity = searchFactory.getIndexBindings().get(Person.class);
      Set<IndexManager> managers = indexBindingForEntity.getIndexManagerSelector().all();
      assertEquals(1, managers.size());
      IndexManager manager = managers.iterator().next();
      assertNotNull(manager);
      assertTrue(manager instanceof DirectoryBasedIndexManager);
      DirectoryBasedIndexManager dbim = (DirectoryBasedIndexManager) manager;
      assertTrue(dbim.getDirectoryProvider() instanceof RAMDirectoryProvider);
   }
}
