package org.infinispan.client.hotrod.admin;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_PROTOSTREAM_TYPE;
import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;

import java.util.List;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.Search;
import org.infinispan.client.hotrod.annotation.model.Model;
import org.infinispan.client.hotrod.annotation.model.ModelA;
import org.infinispan.client.hotrod.annotation.model.ModelB;
import org.infinispan.client.hotrod.annotation.model.SchemaA;
import org.infinispan.client.hotrod.annotation.model.SchemaB;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.commons.marshall.ProtoStreamMarshaller;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.remote.client.ProtobufMetadataManagerConstants;
import org.infinispan.server.core.admin.embeddedserver.EmbeddedServerAdminOperationHandler;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "client.hotrod.admin.SchemaIndexUpdateTest")
public class SchemaIndexUpdateTest extends SingleHotRodServerTest {

   private static final String CACHE_NAME = "models";

   private final ProtoStreamMarshaller schemaEvolutionClientMarshaller = new ProtoStreamMarshaller();

   /**
    * Configure server side (embedded) cache
    *
    * @return the embedded cache manager
    * @throws Exception
    */
   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder
            .encoding()
               .mediaType(APPLICATION_PROTOSTREAM_TYPE)
            .indexing()
               .enable()
               .storage(LOCAL_HEAP)
               .addIndexedEntity("model.Model");

      EmbeddedCacheManager cacheManager = TestCacheManagerFactory.createServerModeCacheManager();
      cacheManager.defineConfiguration(CACHE_NAME, builder.build());

      return cacheManager;
   }

   /**
    * Configure the server, enabling the admin operations
    *
    * @return the HotRod server
    */
   @Override
   protected HotRodServer createHotRodServer() {
      HotRodServerConfigurationBuilder serverBuilder = new HotRodServerConfigurationBuilder();
      serverBuilder.adminOperationsHandler(new EmbeddedServerAdminOperationHandler());

      return HotRodClientTestingUtil.startHotRodServer(cacheManager, serverBuilder);
   }

   /**
    * Configure client side (remote) cache
    *
    * @param host The used host name
    * @param serverPort The used server port
    * @return the HotRod client configuration
    */
   @Override
   protected org.infinispan.client.hotrod.configuration.ConfigurationBuilder createHotRodClientConfigurationBuilder(String host, int serverPort) {
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder builder = HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      builder.addServer()
            .host(host)
            .port(serverPort)
            .addContextInitializer(SchemaA.INSTANCE)
            .marshaller(schemaEvolutionClientMarshaller);
      return builder;
   }

   @Test
   public void test() {
      RemoteCache<Integer, Model> cache = remoteCacheManager.getCache(CACHE_NAME);

      cache.put(1, new ModelA("Fabio"));

      Query<Model> query = Search.getQueryFactory(cache).create("from model.Model where original is not null");
      List<Model> result = query.execute().list();

      assertThat(result).extracting("original").containsExactly("Fabio");
      assertThat(result).hasOnlyElementsOfType(ModelA.class);

      updateSchemaIndex(); // test target

      cache.put(2, new ModelB("Silvia", "Silvia"));

      query = Search.getQueryFactory(cache).create("from model.Model where original is not null");
      result = query.execute().list();

      assertThat(result).extracting("original").containsExactly("Fabio", "Silvia");
      assertThat(result).hasOnlyElementsOfType(ModelB.class);

      query = Search.getQueryFactory(cache).create("from model.Model where different is not null");
      result = query.execute().list();

      assertThat(result).extracting("different").containsExactly("Silvia");
      assertThat(result).hasOnlyElementsOfType(ModelB.class);
   }

   private void updateSchemaIndex() {
      SchemaB schema = SchemaB.INSTANCE;

      // Register proto schema && entity marshaller on client side
      schema.registerSchema(schemaEvolutionClientMarshaller.getSerializationContext());
      schema.registerMarshallers(schemaEvolutionClientMarshaller.getSerializationContext());

      // Register proto schema on server side
      RemoteCache<String, String> metadataCache = remoteCacheManager
            .getCache(ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME);
      metadataCache.put(schema.getProtoFileName(), schema.getProtoFile());

      // reindexCache would make this test working as well,
      // the difference is that with updateIndexSchema the index state (Lucene directories) is not touched,
      // if the schema change is not retro-compatible reindexCache is required
      remoteCacheManager.administration().updateIndexSchema(CACHE_NAME);
   }
}
