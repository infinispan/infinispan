package org.infinispan.client.hotrod.query;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;

import java.io.IOException;
import java.util.List;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.Search;
import org.infinispan.client.hotrod.marshall.MarshallerUtil;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.protostream.FileDescriptorSource;
import org.infinispan.protostream.MessageMarshaller;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.annotations.ProtoDoc;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoName;
import org.infinispan.protostream.annotations.ProtoSchemaBuilder;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.remote.client.ProtobufMetadataManagerConstants;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * Tests for remote queries over HotRod using protostream annotations on a local cache using indexing in RAM.
 *
 * @author Adrian Nistor
 */
@Test(testName = "client.hotrod.query.RemoteQueryWithProtostreamAnnotationsTest", groups = "functional")
public class RemoteQueryWithProtostreamAnnotationsTest extends SingleHotRodServerTest {

   @ProtoDoc("@Indexed")
   @ProtoName("Memo")
   public static class Memo {

      private int id;

      private String text;

      private Author author;

      public Memo(int id, String text) {
         this.id = id;
         this.text = text;
      }

      public Memo() {
      }

      @ProtoDoc("@Field(index = Index.NO, store = Store.NO)")
      @ProtoField(number = 10, required = true)
      public int getId() {
         return id;
      }

      public void setId(int id) {
         this.id = id;
      }

      @ProtoDoc("@Field(store = Store.YES)")
      @ProtoField(number = 20)
      public String getText() {
         return text;
      }

      public void setText(String text) {
         this.text = text;
      }

      @ProtoDoc("@Field(store = Store.YES)")
      @ProtoField(number = 30)
      public Author getAuthor() {
         return author;
      }

      public void setAuthor(Author author) {
         this.author = author;
      }

      @Override
      public String toString() {
         return "Memo{id=" + id + ", text='" + text + '\'' + ", author=" + author + '}';
      }
   }

   public static class Author {

      private int id;

      private String name;

      public Author(int id, String name) {
         this.id = id;
         this.name = name;
      }

      public Author() {
      }

      public int getId() {
         return id;
      }

      public void setId(int id) {
         this.id = id;
      }

      public String getName() {
         return name;
      }

      public void setName(String name) {
         this.name = name;
      }

      @Override
      public String toString() {
         return "Author{id=" + id + ", name='" + name + "'}";
      }
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      org.infinispan.configuration.cache.ConfigurationBuilder builder = new org.infinispan.configuration.cache.ConfigurationBuilder();
      builder.indexing().enable()
             .addIndexedEntity("Memo")
             .addProperty("default.directory_provider", "local-heap")
             .addProperty("lucene_version", "LUCENE_CURRENT");

      EmbeddedCacheManager manager = TestCacheManagerFactory.createServerModeCacheManager();

      manager.defineConfiguration("test", builder.build());

      return manager;
   }

   @Override
   protected RemoteCacheManager getRemoteCacheManager() {
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder = HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      clientBuilder.addServer().host("127.0.0.1").port(hotrodServer.getPort());
      RemoteCacheManager remoteCacheManager = new RemoteCacheManager(clientBuilder.build());
      try {
         registerProtobufSchema(remoteCacheManager);
      } catch (Exception e) {
         throw new RuntimeException(e);
      }
      return remoteCacheManager;
   }

   protected void registerProtobufSchema(RemoteCacheManager remoteCacheManager) throws Exception {
      //initialize client-side serialization context
      String authorSchemaFile = "/* @Indexed */\n" +
            "message Author {\n" +
            "   required int32 id = 1;\n" +
            "   /* @Field(store = Store.YES) */\n" +
            "   required string name = 2;\n" +
            "}";
      SerializationContext serializationContext = MarshallerUtil.getSerializationContext(remoteCacheManager);
      serializationContext.registerProtoFiles(FileDescriptorSource.fromString("author.proto", authorSchemaFile));
      serializationContext.registerMarshaller(new MessageMarshaller<Author>() {
         @Override
         public Author readFrom(ProtoStreamReader reader) throws IOException {
            int id = reader.readInt("id");
            String name = reader.readString("name");
            Author author = new Author();
            author.setId(id);
            author.setName(name);
            return author;
         }

         @Override
         public void writeTo(ProtoStreamWriter writer, Author author) throws IOException {
            writer.writeInt("id", author.getId());
            writer.writeString("name", author.getName());
         }

         @Override
         public Class<Author> getJavaClass() {
            return Author.class;
         }

         @Override
         public String getTypeName() {
            return "Author";
         }
      });

      ProtoSchemaBuilder protoSchemaBuilder = new ProtoSchemaBuilder();
      String memoSchemaFile = protoSchemaBuilder.fileName("memo.proto")
            .addClass(Memo.class)
            .build(serializationContext);

      //initialize server-side serialization context
      RemoteCache<String, String> metadataCache = remoteCacheManager.getCache(ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME);
      metadataCache.put("author.proto", authorSchemaFile);
      metadataCache.put("memo.proto", memoSchemaFile);
      RemoteQueryTestUtils.checkSchemaErrors(metadataCache);
   }

   public void testAttributeQuery() {
      RemoteCache<Integer, Memo> remoteCache = remoteCacheManager.getCache("test");

      remoteCache.put(1, createMemo1());
      remoteCache.put(2, createMemo2());

      // get memo1 back from remote cache and check its attributes
      Memo fromCache = remoteCache.get(1);
      assertMemo1(fromCache);

      // get memo1 back from remote cache via query and check its attributes
      QueryFactory qf = Search.getQueryFactory(remoteCache);
      Query<Memo> query = qf.from(Memo.class)
            .having("text").like("%ipsum%")
            .build();
      List<Memo> list = query.execute().list();
      assertNotNull(list);
      assertEquals(1, list.size());
      assertEquals(Memo.class, list.get(0).getClass());
      assertMemo1(list.get(0));

      // get memo2 back from remote cache via query and check its attributes
      query = qf.from(Memo.class)
            .having("author.name").eq("Adrian")
            .build();
      list = query.execute().list();
      assertNotNull(list);
      assertEquals(1, list.size());
      assertEquals(Memo.class, list.get(0).getClass());
      assertMemo2(list.get(0));
   }

   private Memo createMemo1() {
      Memo memo = new Memo(1, "Lorem ipsum");
      memo.setAuthor(new Author(1, "Tom"));
      return memo;
   }

   private Memo createMemo2() {
      Memo memo = new Memo(2, "Sed ut perspiciatis unde omnis iste natus error");
      memo.setAuthor(new Author(2, "Adrian"));
      return memo;
   }

   private void assertMemo1(Memo memo) {
      assertNotNull(memo);
      assertEquals(1, memo.getId());
      assertEquals("Lorem ipsum", memo.getText());
      assertEquals(1, memo.getAuthor().getId());
   }

   private void assertMemo2(Memo memo) {
      assertNotNull(memo);
      assertEquals(2, memo.getId());
      assertEquals("Sed ut perspiciatis unde omnis iste natus error", memo.getText());
      assertEquals(2, memo.getAuthor().getId());
   }
}
