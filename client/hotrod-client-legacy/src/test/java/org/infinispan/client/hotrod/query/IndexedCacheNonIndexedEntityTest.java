package org.infinispan.client.hotrod.query;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killRemoteCacheManager;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killServers;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.newRemoteConfigurationBuilder;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.registerSCI;
import static org.infinispan.commons.api.CacheContainerAdmin.AdminFlag.VOLATILE;

import org.infinispan.api.annotations.indexing.Basic;
import org.infinispan.api.annotations.indexing.Indexed;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.commons.configuration.StringConfiguration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.protostream.GeneratedSchema;
import org.infinispan.protostream.annotations.AutoProtoSchemaBuilder;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoName;
import org.infinispan.server.core.admin.embeddedserver.EmbeddedServerAdminOperationHandler;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

/**
 * @since 12.0
 */
@Test(testName = "client.hotrod.query.IndexedCacheNonIndexedEntityTest", groups = "functional")
public class IndexedCacheNonIndexedEntityTest extends SingleCacheManagerTest {
   private static final String CACHE_NAME = "IndexedCacheNonIndexedEntitiesTest";

   protected HotRodServer hotRodServer;
   protected RemoteCacheManager remoteCacheManager;

   @ProtoName("Entity")
   static class Entity {
      private final String name;

      @ProtoFactory
      public Entity(String name) {
         this.name = name;
      }

      @ProtoField(1)
      public String getName() {
         return name;
      }
   }

   @Indexed
   @ProtoName("EvilTwin")
   static class EvilTwin {

      @Basic
      @ProtoField(1)
      public String name;
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      cacheManager = TestCacheManagerFactory.createCacheManager(new ConfigurationBuilder());

      HotRodServerConfigurationBuilder serverBuilder = new HotRodServerConfigurationBuilder();
      serverBuilder.adminOperationsHandler(new EmbeddedServerAdminOperationHandler());
      hotRodServer = HotRodClientTestingUtil.startHotRodServer(cacheManager, serverBuilder);

      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder = newRemoteConfigurationBuilder();
      clientBuilder.addServer().host("127.0.0.1").port(hotRodServer.getPort());
      remoteCacheManager = new RemoteCacheManager(clientBuilder.build());
      registerSCI(remoteCacheManager, IndexedCacheNonIndexedEntityTestSCI.INSTANCE);
      return cacheManager;
   }

   @AfterClass(alwaysRun = true)
   public void release() {
      killRemoteCacheManager(remoteCacheManager);
      killServers(hotRodServer);
   }

   @Test(expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = ".*The configured indexed-entity type 'Entity' must be indexed.*")
   public void shouldPreventNonIndexedEntities() {
      String config =
            "<infinispan>" +
                  "  <cache-container>" +
                  "     <local-cache name=\"" + CACHE_NAME + "\">\n" +
                  "        <encoding media-type=\"application/x-protostream\"/>\n" +
                  "        <indexing storage=\"local-heap\">\n" +
                  "           <indexed-entities>\n" +
                  "              <indexed-entity>Entity</indexed-entity>\n" +
                  "              <indexed-entity>EvilTwin</indexed-entity>\n" +
                  "           </indexed-entities>\n" +
                  "          </indexing>" +
                  "       </local-cache>" +
                  "  </cache-container>" +
                  "</infinispan>";

      remoteCacheManager.administration().withFlags(VOLATILE).createCache(CACHE_NAME, new StringConfiguration(config));

      // The SearchMapping is started lazily for protobuf caches, so the exception only happens after first usage
      RemoteCache<Object, Object> cache = remoteCacheManager.getCache(CACHE_NAME);
      cache.put("1", new Entity("name"));
   }

   @AutoProtoSchemaBuilder(
         includeClasses = {Entity.class, EvilTwin.class},
         schemaFileName = "test.client.IndexedCacheNonIndexedEntityTest.proto",
         schemaFilePath = "proto/generated",
         service = false
   )
   public interface IndexedCacheNonIndexedEntityTestSCI extends GeneratedSchema {
      GeneratedSchema INSTANCE = new IndexedCacheNonIndexedEntityTestSCIImpl();
   }
}
