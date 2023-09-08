package org.infinispan;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.Search;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.remote.client.ProtobufMetadataManagerConstants;

public class RemoteQuery {

   public static void main(String[] args) throws Exception {
      ConfigurationBuilder clientBuilder = new ConfigurationBuilder();
      // RemoteQueryInitializerImpl is generated
      clientBuilder.addServer().host("127.0.0.1").port(11222)
            .security().authentication().username("user").password("user")
            .addContextInitializers(new RemoteQueryInitializerImpl());

      RemoteCacheManager remoteCacheManager = new RemoteCacheManager(clientBuilder.build());

      // Grab the generated protobuf schema and registers in the server.
      Path proto = Paths.get(RemoteQuery.class.getClassLoader()
            .getResource("proto/book.proto").toURI());
      String protoBufCacheName = ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME;
      remoteCacheManager.getCache(protoBufCacheName).put("book.proto", Files.readString(proto));

      // Obtain the 'books' remote cache
      RemoteCache<Object, Object> remoteCache = remoteCacheManager.getCache("books");

      // Add some Books
      Book book1 = new Book("Infinispan in Action", "Learn Infinispan with using it", 2015);
      Book book2 = new Book("Cloud-Native Applications with Java and Quarkus", "Build robust and reliable cloud applications", 2019);

      remoteCache.put(1, book1);
      remoteCache.put(2, book2);

      // Execute a full-text query
      Query<Book> query = remoteCache.query("FROM book_sample.Book WHERE title:'java'");

      List<Book> list = query.execute().list(); // Voila! We have our book back from the cache!
   }
}
