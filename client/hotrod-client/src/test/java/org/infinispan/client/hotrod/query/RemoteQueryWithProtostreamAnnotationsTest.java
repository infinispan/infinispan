package org.infinispan.client.hotrod.query;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;

import java.io.IOException;
import java.util.List;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.Search;
import org.infinispan.client.hotrod.marshall.ProtoStreamMarshaller;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.configuration.cache.Index;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.protostream.FileDescriptorSource;
import org.infinispan.protostream.MessageMarshaller;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.annotations.ProtoDoc;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoMessage;
import org.infinispan.protostream.annotations.ProtoSchemaBuilder;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.remote.client.ProtobufMetadataManagerConstants;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Tests for remote queries over HotRod using protostream annotations on a local cache using RAM directory.
 *
 * @author Adrian Nistor
 */
@Test(testName = "client.hotrod.query.RemoteQueryWithProtostreamAnnotationsTest", groups = "functional")
public class RemoteQueryWithProtostreamAnnotationsTest extends SingleHotRodServerTest {

   @ProtoDoc("@Indexed")
   @ProtoMessage(name = "Memo")
   public static class Memo {

      private int id;

      private String text;

      private Author author;

      @ProtoDoc("@Field(index = Index.NO, store = Store.NO)")
      @ProtoField(number = 10, required = true)
      public int getId() {
         return id;
      }

      public void setId(int id) {
         this.id = id;
      }

      @ProtoDoc("@IndexedField")
      @ProtoField(number = 20)
      public String getText() {
         return text;
      }

      public void setText(String text) {
         this.text = text;
      }

      @ProtoDoc("@IndexedField")
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
      builder.indexing().index(Index.ALL)
            .addProperty("default.directory_provider", "local-heap")
            .addProperty("lucene_version", "LUCENE_CURRENT");

      return TestCacheManagerFactory.createServerModeCacheManager(builder);
   }

   @Override
   protected RemoteCacheManager getRemoteCacheManager() {
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder = new org.infinispan.client.hotrod.configuration.ConfigurationBuilder();
      clientBuilder.addServer().host("127.0.0.1").port(hotrodServer.getPort());
      clientBuilder.marshaller(new ProtoStreamMarshaller());
      return new RemoteCacheManager(clientBuilder.build());
   }

   @BeforeClass
   protected void registerProtobufSchema() throws Exception {
      //initialize client-side serialization context
      String authorSchemaFile = "/* @Indexed */\n" +
            "message Author {\n" +
            "   required int32 id = 1;\n" +
            "   /* @IndexedField */\n" +
            "   required string name = 2;\n" +
            "}";
      SerializationContext serializationContext = ProtoStreamMarshaller.getSerializationContext(remoteCacheManager);
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
      assertFalse(metadataCache.containsKey(ProtobufMetadataManagerConstants.ERRORS_KEY_SUFFIX));
   }

   public void testAttributeQuery() throws Exception {
      RemoteCache<Integer, Memo> remoteCache = remoteCacheManager.getCache();

      remoteCache.put(1, createMemo1());
      remoteCache.put(2, createMemo2());

      // get memo1 back from remote cache and check its attributes
      Memo fromCache = remoteCache.get(1);
      assertMemo1(fromCache);

      // get memo1 back from remote cache via query and check its attributes
      QueryFactory qf = Search.getQueryFactory(remoteCache);
      Query query = qf.from(Memo.class)
            .having("text").like("%ipsum%")
            .build();
      List<Memo> list = query.list();
      assertNotNull(list);
      assertEquals(1, list.size());
      assertEquals(Memo.class, list.get(0).getClass());
      assertMemo1(list.get(0));

      // get memo2 back from remote cache via query and check its attributes
      query = qf.from(Memo.class)
            .having("author.name").eq("Adrian")
            .build();
      list = query.list();
      assertNotNull(list);
      assertEquals(1, list.size());
      assertEquals(Memo.class, list.get(0).getClass());
      assertMemo2(list.get(0));
   }

   private Memo createMemo1() {
      Author author = new Author();
      author.setId(1);
      author.setName("Tom");
      Memo memo = new Memo();
      memo.setId(1);
      memo.setText("Lorem ipsum");
      memo.setAuthor(author);
      return memo;
   }

   private Memo createMemo2() {
      Author author = new Author();
      author.setId(2);
      author.setName("Adrian");
      Memo memo = new Memo();
      memo.setId(2);
      memo.setText("Sed ut perspiciatis unde omnis iste natus error");
      memo.setAuthor(author);
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
