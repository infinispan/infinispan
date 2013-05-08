/* 
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */
package org.infinispan.query.cacheloaders;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Indexed;
import org.hibernate.search.annotations.Store;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.Search;
import org.infinispan.query.SearchManager;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.Serializable;
import java.util.List;

import static org.infinispan.test.TestingUtil.withCacheManager;
import static org.testng.Assert.assertEquals;

/**
 * Tests persistent index state us in synch with the values stored in a CacheLoader
 * after a CacheManager is restarted.
 * 
 * @author Jan Slezak
 * @author Sanne Grinovero
 * @since 5.2
 */
@Test(groups = "functional", testName = "query.cacheloaders.InconsistentIndexesAfterRestartTest")
public class InconsistentIndexesAfterRestartTest extends AbstractInfinispanTest {

    private static String TMP_DIR;

    @Test
    public void testPutSearchablePersistentWithoutBatchingWithoutTran() throws Exception {
        testPutTwice(false, false);
    }

    @Test
    public void testPutSearchablePersistentWithBatchingWithoutTran() throws Exception {
        testPutTwice(true, false);
    }

    @Test
    public void testPutSearchablePersistentWithBatchingInTran() throws Exception {
        testPutTwice(true, true);
    }

    private void testPutTwice(boolean batching, boolean inTran) throws Exception {
       testPutOperation(batching, inTran);
       testPutOperation(batching, inTran);
    }

    private void testPutOperation(boolean batching, final boolean inTran) throws Exception {
       withCacheManager(new CacheManagerCallable(getCacheManager(batching)) {
          @Override
          public void call() {
             try {
                Cache<Object, Object> c = cm.getCache();
                if (inTran) c.getAdvancedCache().getTransactionManager().begin();
                c.put("key1", new SEntity(1, "name1", "surname1"));
                if (inTran) c.getAdvancedCache().getTransactionManager().commit();
                assertEquals(searchByName("name1", c).size(), 1, "should be 1, even repeating this");
             } catch (Exception e) {
                throw new RuntimeException(e);
             }
          }
       });
    }

    private EmbeddedCacheManager getCacheManager(boolean batchingEnabled) throws Exception {
       ConfigurationBuilder builder = new ConfigurationBuilder();
       builder
          .loaders()
             .passivation(false)
             .preload(false)
          .addFileCacheStore()
             .location(TMP_DIR + File.separator + "cacheStore")
             .fetchPersistentState(true)
             .purgeOnStartup(false)
          .indexing()
             .enable()
             .indexLocalOnly(true)
             .addProperty("default.directory_provider", "filesystem")
             .addProperty("default.indexBase", TMP_DIR + File.separator + "idx");

       if (batchingEnabled) {
          builder.invocationBatching().enable();
       }
       else {
          builder.invocationBatching().disable();
       }

       return TestCacheManagerFactory.createCacheManager(builder);
    }

    private List searchByName(String name, Cache c) {
        SearchManager sm = Search.getSearchManager(c);
        CacheQuery q = sm.getQuery(SEntity.searchByName(name), SEntity.class);
        int resultSize = q.getResultSize();
        List l = q.list();
        assert l.size() == resultSize;
        return q.list();
    }

    @Indexed
    public static class SEntity implements Serializable {

        public static final String IDX_NAME = "name";

        public static final String IDX_SURNAME = "surname";

        private final long id;

        @Field(store = Store.YES)
        private final String name;

        @Field (store = Store.YES)
        private final String surname;

        public SEntity(long id, String name, String surname) {
            this.id = id;
            this.name = name;
            this.surname = surname;
        }

        public long getId() {
            return id;
        }

        public String getName() {
            return name;
        }

        public String getSurname() {
            return surname;
        }

        @Override
        public String toString() {
            return "SEntity{" +
                    "id=" + id +
                    ", name='" + name + '\'' +
                    ", surname='" + surname + '\'' +
                    '}';
        }

        public static Query searchByName(String name) {
            BooleanQuery query = new BooleanQuery();
            query.add(new TermQuery(
                    new Term(SEntity.IDX_NAME, name.toLowerCase())), BooleanClause.Occur.MUST);
            return query;
        }
    }

    @BeforeClass
    protected void setUpTempDir() {
       TMP_DIR = TestingUtil.tmpDirectory(this);
       new File(TMP_DIR).mkdirs();
    }

    @AfterClass
    protected void clearTempDir() {
       TestingUtil.recursiveFileRemove(TMP_DIR);
    }

}