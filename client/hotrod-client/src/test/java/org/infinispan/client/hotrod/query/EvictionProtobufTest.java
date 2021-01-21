package org.infinispan.client.hotrod.query;

import static org.infinispan.configuration.cache.StorageType.HEAP;
import static org.infinispan.eviction.EvictionStrategy.REMOVE;
import static org.testng.Assert.assertEquals;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.IndexStorage;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.AutoProtoSchemaBuilder;
import org.infinispan.protostream.annotations.ProtoDoc;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.server.hotrod.HotRodServer;
import org.testng.annotations.Test;

/**
 * @since 12.0
 */
@Test(groups = "functional", testName = "client.hotrod.query.EvictionProtobufTest")
public class EvictionProtobufTest extends MultiHotRodServersTest {
   private static final int NUM_SERVERS = 2;
   private static final String CACHE_NAME = "test";

   @Override
   protected void createCacheManagers() throws Throwable {
      createHotRodServers(NUM_SERVERS, new ConfigurationBuilder());

      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC);
      builder.encoding().mediaType(MediaType.APPLICATION_PROTOSTREAM_TYPE);
      builder.expiration().wakeUpInterval(60_000);
      builder.memory().storage(HEAP).maxSize("1000000000").whenFull(REMOVE);
      builder.indexing().enable().storage(IndexStorage.LOCAL_HEAP).addIndexedEntities("sample.Book");

      for (HotRodServer server : servers) {
         server.getCacheManager().defineConfiguration(CACHE_NAME, builder.build());
         server.getCacheManager().getCache(CACHE_NAME);
      }
   }

   @Override
   protected SerializationContextInitializer contextInitializer() {
      return TestSCI.INSTANCE;
   }

   @Test
   public void testCacheInteraction() {
      RemoteCacheManager client = client(0);
      RemoteCache<String, Book> cache = client.getCache(CACHE_NAME);

      cache.put("100", new Book("Persepolis Rising", "James Corey", 2017));
      assertEquals(cache.size(), 1);

      cache.put("100", new Book("Nemesis Games", "James Corey", 2015));
      assertEquals(cache.size(), 1);

      cache.remove("100");
      assertEquals(cache.size(), 0);
   }

   @AutoProtoSchemaBuilder(
         includeClasses = Book.class,
         schemaFileName = "test.EvictionProtobufTest.proto",
         schemaFilePath = "proto/generated",
         schemaPackageName = "sample"
   )
   interface TestSCI extends SerializationContextInitializer {
      EvictionProtobufTest.TestSCI INSTANCE = new TestSCIImpl();
   }

   @ProtoDoc("@Indexed")
   public static class Book {

      @ProtoDoc("@Field(index=Index.YES, analyze = Analyze.YES, store = Store.NO)")
      @ProtoField(number = 1)
      final String title;

      @ProtoDoc("@Field(index=Index.YES, analyze = Analyze.YES, store = Store.NO)")
      @ProtoField(number = 2)
      final String author;

      @ProtoDoc("@Field(index=Index.YES, analyze = Analyze.YES, store = Store.NO)")
      @ProtoField(number = 3, defaultValue = "0")
      final int publicationYear;


      @ProtoFactory
      public Book(String title, String author, int publicationYear) {
         this.title = title;
         this.author = author;
         this.publicationYear = publicationYear;
      }
   }

}
