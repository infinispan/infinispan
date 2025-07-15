package org.infinispan.client.hotrod.admin;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;

import java.util.List;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.annotation.model.Image;
import org.infinispan.client.hotrod.annotation.model.Model;
import org.infinispan.client.hotrod.annotation.model.ModelA;
import org.infinispan.client.hotrod.annotation.model.ModelB;
import org.infinispan.client.hotrod.annotation.model.ModelC;
import org.infinispan.client.hotrod.annotation.model.SchemaA;
import org.infinispan.client.hotrod.annotation.model.SchemaB;
import org.infinispan.client.hotrod.annotation.model.SchemaC;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.commons.api.query.Query;
import org.infinispan.commons.marshall.ProtoStreamMarshaller;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.protostream.GeneratedSchema;
import org.infinispan.server.core.admin.embeddedserver.EmbeddedServerAdminOperationHandler;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "client.hotrod.admin.SchemaIndexUpdateTest")
public class SchemaIndexUpdateTest extends SingleHotRodServerTest {

   private static final String CACHE_NAME = "models";

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder
            .indexing()
            .enable()
            .storage(LOCAL_HEAP)
            .addIndexedEntity("model.Model");

      EmbeddedCacheManager cacheManager = TestCacheManagerFactory.createServerModeCacheManager();
      cacheManager.defineConfiguration(CACHE_NAME, builder.build());

      return cacheManager;
   }

   @Override
   protected HotRodServer createHotRodServer() {
      HotRodServerConfigurationBuilder serverBuilder = new HotRodServerConfigurationBuilder();
      serverBuilder.adminOperationsHandler(new EmbeddedServerAdminOperationHandler());

      return HotRodClientTestingUtil.startHotRodServer(cacheManager, serverBuilder);
   }

   @Override
   protected org.infinispan.client.hotrod.configuration.ConfigurationBuilder createHotRodClientConfigurationBuilder(String host, int serverPort) {
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder builder = HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      builder.addServer()
            .host(host)
            .port(serverPort);
      return builder;
   }

   @Test
   public void updateWithoutAndWithOtherEntities() {
      try (RemoteCacheManager rcm = clientForSchema(SchemaA.INSTANCE, null)) {
         RemoteCache<Integer, Object> cache = rcm.getCache(CACHE_NAME);

         cache.put(1, new ModelA("Fabio"));

         Query<Model> query = cache.query("from model.Model where original is not null");
         List<Model> models = query.execute().list();

         assertThat(models).extracting("original").containsExactly("Fabio");
         assertThat(models).hasOnlyElementsOfType(ModelA.class);
      }

      try (RemoteCacheManager rcm = clientForSchema(SchemaB.INSTANCE, null)) {
         RemoteCache<Integer, Object> cache = rcm.getCache(CACHE_NAME);
         cache.put(2, new ModelB("Silvia", "Silvia"));

         Query<Model> query = cache.query("from model.Model where original is not null");
         List<Model> models = query.execute().list();

         assertThat(models).extracting("original").containsExactly("Fabio", "Silvia");
         assertThat(models).hasOnlyElementsOfType(ModelB.class);

         query = cache.query("from model.Model where different is not null");
         models = query.execute().list();

         assertThat(models).extracting("different").containsExactly("Silvia");
         assertThat(models).hasOnlyElementsOfType(ModelB.class);

         Query<Image> imageQuery = cache.query("from model.Image where name is not null");

         // model.Image is not present on the indexedEntities
         assertThatThrownBy(() -> imageQuery.execute().list())
               .isInstanceOf(HotRodClientException.class)
               .hasMessageContaining("Unknown type name : model.Image");
      }

      try (RemoteCacheManager rcm = clientForSchema(SchemaC.INSTANCE, "model.Model model.Image")) {
         RemoteCache<Integer, Object> cache = rcm.getCache(CACHE_NAME);


         cache.put(3, new ModelC("Elena", "Elena", "Elena"));

         Query<Model> query = cache.query("from model.Model where original is not null");
         List<Model> models = query.execute().list();

         assertThat(models).extracting("original").containsExactly("Fabio", "Silvia", "Elena");
         assertThat(models).hasOnlyElementsOfType(ModelC.class);

         query = cache.query("from model.Model where different is not null");
         models = query.execute().list();

         assertThat(models).extracting("different").containsExactly("Silvia", "Elena");
         assertThat(models).hasOnlyElementsOfType(ModelC.class);

         query = cache.query("from model.Model where divergent is not null");
         models = query.execute().list();

         assertThat(models).extracting("divergent").containsExactly("Elena");
         assertThat(models).hasOnlyElementsOfType(ModelC.class);

         // now it is possible to play with model.Image message type too
         Query<Image> imageQuery = cache.query("from model.Image where name is not null");
         List<Image> images = imageQuery.execute().list();
         assertThat(images).isEmpty();

         cache.put(4, new Image("name"));
         images = imageQuery.execute().list();
         assertThat(images).extracting("name").containsExactly("name");
         assertThat(images).hasOnlyElementsOfType(Image.class);
      }
   }

   private RemoteCacheManager clientForSchema(GeneratedSchema schema, String newIndexedEntities) {
      // Register proto schema && entity marshaller on client side
      ProtoStreamMarshaller marshaller = new ProtoStreamMarshaller();
      marshaller.register(schema);

      org.infinispan.client.hotrod.configuration.ConfigurationBuilder builder = createHotRodClientConfigurationBuilder(hotrodServer.getHost(), hotrodServer.getPort());
      builder.marshaller(marshaller);
      RemoteCacheManager rcm = new RemoteCacheManager(builder.build());

      // Register proto schema on server side
      assertThat(rcm.administration().schemas().createOrUpdate(schema).isCreated()).isTrue();

      if (newIndexedEntities != null) {
         remoteCacheManager.administration().withFlags(CacheContainerAdmin.AdminFlag.VOLATILE)
               .updateConfigurationAttribute(CACHE_NAME, "indexing.indexed-entities", newIndexedEntities);
      }

      // reindexCache would make this test working as well,
      // the difference is that with updateIndexSchema the index state (Lucene directories) is not touched,
      // if the schema change is not retro-compatible reindexCache is required
      rcm.administration().updateIndexSchema(CACHE_NAME);

      return rcm;
   }
}
