package org.infinispan.query.persistence;

import static org.infinispan.test.TestingUtil.withCacheManager;
import static org.testng.Assert.assertEquals;

import java.io.File;
import java.io.Serializable;
import java.nio.file.Paths;
import java.util.List;

import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Indexed;
import org.hibernate.search.annotations.Store;
import org.infinispan.Cache;
import org.infinispan.commons.test.CommonsTestingUtil;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.AutoProtoSchemaBuilder;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.Search;
import org.infinispan.query.SearchManager;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Tests persistent index state us in synch with the values stored in a CacheLoader
 * after a CacheManager is restarted.
 *
 * @author Jan Slezak
 * @author Sanne Grinovero
 * @since 5.2
 */
@Test(groups = "functional", testName = "query.persistence.InconsistentIndexesAfterRestartTest")
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

    private void testPutOperation(boolean batching, final boolean inTran) {
       withCacheManager(new CacheManagerCallable(getCacheManager(batching)) {
          @Override
          public void call() throws Exception {
             Cache<Object, Object> c = cm.getCache();
             if (inTran) c.getAdvancedCache().getTransactionManager().begin();
             c.put("key1", new SEntity(1, "name1", "surname1"));
             if (inTran) c.getAdvancedCache().getTransactionManager().commit();
             assertEquals(searchByName("name1", c).size(), 1, "should be 1, even repeating this");
          }
       });
    }

    private EmbeddedCacheManager getCacheManager(boolean batchingEnabled) {
       GlobalConfigurationBuilder globalBuilder = new GlobalConfigurationBuilder().nonClusteredDefault();
       globalBuilder.globalState().persistentLocation(TMP_DIR);
       globalBuilder.serialization().addContextInitializer(SCI.INSTANCE);

       ConfigurationBuilder builder = new ConfigurationBuilder();
       builder
          .persistence()
          .addSingleFileStore()
             .fetchPersistentState(true)
          .indexing()
             .enable()
             .addIndexedEntity(SEntity.class)
             .addProperty("default.directory_provider", "filesystem")
             .addProperty("lucene_version", "LUCENE_CURRENT")
             .addProperty("default.indexBase", Paths.get(TMP_DIR, "idx").toString())
          .invocationBatching()
             .enable(batchingEnabled);

       return TestCacheManagerFactory.createCacheManager(globalBuilder, builder);
    }

    private List searchByName(String name, Cache c) {
        SearchManager sm = Search.getSearchManager(c);
        CacheQuery<?> q = sm.getQuery(SEntity.searchByName(name));
        int resultSize = q.getResultSize();
        List<?> l = q.list();
        assert l.size() == resultSize;
        return q.list();
    }

    @Indexed
    public static class SEntity implements Serializable {

        public static final String IDX_NAME = "name";

        private final long id;

        @Field(store = Store.YES)
        private final String name;

        @Field (store = Store.YES)
        private final String surname;

        @ProtoFactory
        SEntity(long id, String name, String surname) {
            this.id = id;
            this.name = name;
            this.surname = surname;
        }

        @ProtoField(number = 1, defaultValue = "0")
        public long getId() {
            return id;
        }

        @ProtoField(number = 2)
        public String getName() {
            return name;
        }

        @ProtoField(number = 3)
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

       public static String searchByName(String name) {
           return String.format("FROM %s WHERE %s:'%s'", SEntity.class.getName(), IDX_NAME, name.toLowerCase());
       }
    }

    @BeforeClass
    protected void setUpTempDir() {
       TMP_DIR = CommonsTestingUtil.tmpDirectory(this.getClass());
       new File(TMP_DIR).mkdirs();
    }

    @AfterClass
    protected void clearTempDir() {
       Util.recursiveFileRemove(TMP_DIR);
    }

   @AutoProtoSchemaBuilder(
         includeClasses = SEntity.class,
         schemaFileName = "test.query.persistence.InconsistentIndexesAfterRestartTest.proto",
         schemaFilePath = "proto/generated",
         schemaPackageName = "org.infinispan.test.InconsistentIndexesAfterRestartTest")
   interface SCI extends SerializationContextInitializer {
      InconsistentIndexesAfterRestartTest.SCI INSTANCE = new SCIImpl();
   }
}
