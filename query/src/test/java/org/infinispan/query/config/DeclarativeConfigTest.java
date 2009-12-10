package org.infinispan.query.config;

import org.apache.lucene.queryParser.ParseException;
import org.infinispan.manager.CacheManager;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.QueryFactory;
import org.infinispan.query.backend.QueryHelper;
import org.infinispan.query.helper.TestQueryHelperFactory;
import org.infinispan.query.test.Person;
import org.infinispan.test.SingleCacheManagerTest;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.List;

@Test(testName = "query.config.DeclarativeConfigTest", groups = "functional")
public class DeclarativeConfigTest extends SingleCacheManagerTest {

   QueryFactory qf;

   @Override
   protected CacheManager createCacheManager() throws Exception {
      String config = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
            "\n" +
            "<infinispan xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns=\"urn:infinispan:config:4.0\" xmlns:query=\"urn:infinispan:config:query:4.0\">\n" +
            "   <default>\n" +
            "      <indexing enabled=\"true\" indexLocalOnly=\"true\"/>\n" +
            "   </default>\n" +
            "</infinispan>";

      InputStream is = new ByteArrayInputStream(config.getBytes());
      cacheManager = new DefaultCacheManager(is, true);
      cache = cacheManager.getCache();
      QueryHelper qh = TestQueryHelperFactory.createTestQueryHelperInstance(cache, Person.class);
      qf = new QueryFactory(cache, qh);
      return cacheManager;
   }

   public void simpleIndexTest() throws ParseException {
      cache.put("1", new Person("A Person's Name", "A paragraph containing some text", 75));
      CacheQuery cq = qf.getBasicQuery("name", "Person");
      assert cq.getResultSize() == 1;
      List<Object> l =  cq.list();
      assert l.size() == 1;
      Person p = (Person) l.get(0);
      assert p.getName().equals("A Person's Name");
      assert p.getBlurb().equals("A paragraph containing some text");
      assert p.getAge() == 75;
   }
}
