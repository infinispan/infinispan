/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009, Red Hat Middleware LLC, and individual contributors
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.query.config;

import org.apache.lucene.queryParser.ParseException;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.backend.QueryHelper;
import org.infinispan.query.helper.TestQueryHelperFactory;
import org.infinispan.query.test.Person;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.List;

@Test(testName = "query.config.DeclarativeConfigTest", groups = "functional")
public class DeclarativeConfigTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      String config = TestingUtil.INFINISPAN_START_TAG +
            "   <default>\n" +
            "      <indexing enabled=\"true\" indexLocalOnly=\"true\"/>\n" +
            "   </default>\n" + TestingUtil.INFINISPAN_END_TAG;

      InputStream is = new ByteArrayInputStream(config.getBytes());
      cacheManager = TestCacheManagerFactory.fromStream(is);
      cache = cacheManager.getCache();
      return cacheManager;
   }

   public void simpleIndexTest() throws ParseException {
      QueryHelper qh = TestQueryHelperFactory.createTestQueryHelperInstance(cache, Person.class);
      cache.put("1", new Person("A Person's Name", "A paragraph containing some text", 75));
      CacheQuery cq = TestQueryHelperFactory.createCacheQuery(cache, qh, "name", "Person");
      assert cq.getResultSize() == 1;
      List<Object> l =  cq.list();
      assert l.size() == 1;
      Person p = (Person) l.get(0);
      assert p.getName().equals("A Person's Name");
      assert p.getBlurb().equals("A paragraph containing some text");
      assert p.getAge() == 75;
   }
}
