package org.infinispan.client.hotrod.query;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;

import java.util.Objects;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.marshall.MarshallerUtil;
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.annotations.ProtoDoc;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoSchemaBuilder;
import org.infinispan.query.remote.client.ProtobufMetadataManagerConstants;
import org.testng.annotations.Test;

/**
 * Reproducer for https://issues.jboss.org/browse/ISPN-9068
 */
@Test(testName = "client.hotrod.query.MultiServerStoreQueryTest", groups = "functional")
public class MultiServerStoreQueryTest extends MultiHotRodServersTest {

   private static final int NODES = 2;
   private static final boolean USE_PERSISTENCE = true;

   private static final String USER_CACHE = "news";

   private RemoteCache<Object, Object> userCache;

   private long evictionSize = -1;

   @Override
   protected String parameters() {
      return "[" + storageType + ":" + evictionSize + "]";
   }

   @Override
   protected String[] parameterNames() {
      return concat(super.parameterNames(), "STORAGE-TYPE", "EVICTION_SIZE");
   }

   @Override
   protected Object[] parameterValues() {
      return concat(super.parameterValues(), storageType, evictionSize);
   }

   public Object[] factory() {
      return new Object[]{
            new MultiServerStoreQueryTest().storageType(StorageType.OFF_HEAP),
            new MultiServerStoreQueryTest().storageType(StorageType.BINARY),
            new MultiServerStoreQueryTest().storageType(StorageType.OBJECT),
            new MultiServerStoreQueryTest().storageType(StorageType.OFF_HEAP).evictionSize(1),
            new MultiServerStoreQueryTest().storageType(StorageType.BINARY).evictionSize(1),
            new MultiServerStoreQueryTest().storageType(StorageType.OBJECT).evictionSize(1),
      };
   }

   public MultiServerStoreQueryTest storageType(StorageType storageType) {
      this.storageType = storageType;
      return this;
   }

   MultiServerStoreQueryTest evictionSize(long size) {
      evictionSize = size;
      return this;
   }

   public Configuration getLockCacheConfig() {
      return getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false).build();
   }

   public Configuration getLuceneCacheConfig(String storeName) {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false);
      if (USE_PERSISTENCE) {
         builder.persistence().addStore(DummyInMemoryStoreConfigurationBuilder.class).preload(true).storeName(storeName);
      }
      return builder.build();
   }

   public Configuration buildIndexedConfig(String storeName) {
      ConfigurationBuilder builder = hotRodCacheConfiguration(getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false));
      builder.indexing().enable()
             .addIndexedEntity("News")
             .addProperty("default.directory_provider", "local-heap")
             .addProperty("default.worker.execution", "async")
             .addProperty("default.index_flush_interval", "500")
             .addProperty("default.indexwriter.merge_factor", "30")
             .addProperty("default.indexwriter.merge_max_size", "1024")
             .addProperty("default.indexwriter.ram_buffer_size", "256");
      builder.memory().storageType(storageType);
      if (evictionSize > 0) {
         builder.memory().size(evictionSize);
      }
      if (USE_PERSISTENCE)
         builder.persistence().addStore(DummyInMemoryStoreConfigurationBuilder.class).preload(true).storeName(storeName);

      return builder.build();
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder defaultConfiguration = new ConfigurationBuilder();
      createHotRodServers(NODES, defaultConfiguration);

      RemoteCacheManager remoteCacheManager = client(0);

      //initialize client-side serialization context
      SerializationContext serializationContext = MarshallerUtil.getSerializationContext(remoteCacheManager);
      ProtoSchemaBuilder protoSchemaBuilder = new ProtoSchemaBuilder();
      String protoFile = protoSchemaBuilder.fileName("news.proto")
                                           .addClass(News.class)
                                           .addClass(NewsKey.class)
                                           .build(serializationContext);

      //initialize server-side serialization context
      RemoteCache<String, String> metadataCache = remoteCacheManager.getCache(ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME);
      metadataCache.put("news.proto", protoFile);
      assertFalse(metadataCache.containsKey(ProtobufMetadataManagerConstants.ERRORS_KEY_SUFFIX));

      for (int i = 0; i < cacheManagers.size(); i++) {
         EmbeddedCacheManager cm = cacheManagers.get(i);
         cm.defineConfiguration(USER_CACHE, buildIndexedConfig("News-" + i));
         cm.getCache(USER_CACHE);
      }

      waitForClusterToForm(USER_CACHE);

      userCache = remoteCacheManager.getCache(USER_CACHE);
   }

   public void testIndexing() {
      News news = new News();
      news.setId("testnews");
      news.setTimestamp(0);

      userCache.put(news.getId(), news);

      Object testNews = userCache.get("testnews");
      assertEquals(news, testNews);
   }

   public void testNonPrimitiveKey() {
      NewsKey newsKey1 = new NewsKey();
      newsKey1.setArticle("articleKey1");

      NewsKey newsKey2 = new NewsKey();
      newsKey2.setArticle("articleKey2");

      News news1 = new News();
      news1.setId("test-news-1");
      news1.setTimestamp(0);

      News news2 = new News();
      news2.setId("test-news-2");
      news2.setTimestamp(0);

      userCache.put(newsKey1, news1);
      userCache.put(newsKey2, news2);

      assertEquals(news1, userCache.get(newsKey1));
      assertEquals(news2, userCache.get(newsKey2));
   }
}

class NewsKey {
   private String article;

   NewsKey() {
   }

   @ProtoField(number = 1, required = true)
   public void setArticle(String article) {
      this.article = article;
   }

   public String getArticle() {
      return article;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      NewsKey newsKey = (NewsKey) o;
      return Objects.equals(article, newsKey.article);
   }

   @Override
   public int hashCode() {
      return Objects.hash(article);
   }
}

@ProtoDoc("@Indexed")
class News {
   private String id;
   private long timestamp;

   public News() {
   }

   public String getId() {
      return id;
   }

   @ProtoField(number = 1, required = true)
   public void setId(String id) {
      this.id = id;
   }

   @ProtoDoc("@Field(index=Index.YES, analyze = Analyze.NO, store = Store.NO)")
   @ProtoDoc("@SortableField")
   @ProtoField(number = 2, required = true)
   public long getTimestamp() {
      return timestamp;
   }

   public void setTimestamp(long time) {
      this.timestamp = time;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) {
         return true;
      }
      if (o == null || getClass() != o.getClass()) {
         return false;
      }
      News news = (News) o;
      return timestamp == news.timestamp && Objects.equals(id, news.id);
   }

   @Override
   public int hashCode() {
      return Objects.hash(id, timestamp);
   }
}
