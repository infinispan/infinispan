package org.infinispan.client.hotrod.query;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.infinispan.commons.api.CacheContainerAdmin.AdminFlag.VOLATILE;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_PROTOSTREAM_TYPE;
import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;
import static org.infinispan.query.remote.client.ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME;
import static org.testng.AssertJUnit.assertEquals;

import org.infinispan.client.hotrod.DataFormat;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.Search;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.commons.configuration.XMLStringConfiguration;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.query.dsl.Query;
import org.infinispan.server.core.admin.embeddedserver.EmbeddedServerAdminOperationHandler;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.testng.annotations.Test;

/**
 * Tests protobuf mapping with indexed fields and non-indexed repeated field.
 *
 * @since 12.1
 */
@Test(testName = "client.hotrod.query.RemoteQueryRepeatedMappingTest", groups = "functional")
public class RemoteQueryRepeatedMappingTest extends SingleHotRodServerTest {
   private static final String CACHE_NAME = RemoteQueryRepeatedMappingTest.class.getName();
   private static final String SCHEMA_FILE = "indexed-repeated.proto";

   @Override
   protected HotRodServer createHotRodServer() {
      HotRodServerConfigurationBuilder serverBuilder = new HotRodServerConfigurationBuilder();
      serverBuilder.adminOperationsHandler(new EmbeddedServerAdminOperationHandler());
      return HotRodClientTestingUtil.startHotRodServer(cacheManager, serverBuilder);
   }

   @Override
   protected RemoteCacheManager getRemoteCacheManager() {
      return super.getRemoteCacheManager();
   }

   @Test
   public void testCreateAndQuery() throws Exception {
      registerProtoBuf();

      RemoteCache<Object, Object> cache = remoteCacheManager.administration().withFlags(VOLATILE)
            .createCache(CACHE_NAME, createCacheXMLConfig());

      DataFormat dataFormat = DataFormat.builder().keyType(APPLICATION_JSON).valueType(APPLICATION_JSON).build();

      RemoteCache<byte[], byte[]> jsonCache = cache.withDataFormat(dataFormat);

      jsonCache.put(keyAsJson(), valueAsJson());

      Query<Object> querySlowChildren = Search.getQueryFactory(cache).create("SELECT COUNT(*) FROM Parent p WHERE p.slowChildren.id = 0");
      Query<Object> queryFastChildren = Search.getQueryFactory(cache).create("SELECT COUNT(*) FROM Parent p WHERE p.fastChildren.id = 10");
      Query<Object> queryFieldChildren = Search.getQueryFactory(cache).create("SELECT COUNT(*) FROM Parent p WHERE p.fieldLessChildren.id = 0");

      assertEquals(1, querySlowChildren.execute().hitCount().orElse(-1));
      assertEquals(1, queryFastChildren.execute().hitCount().orElse(-1));
      assertEquals(1, queryFieldChildren.execute().hitCount().orElse(-1));
   }

   private byte[] keyAsJson() {
      return Json.object().set("_type", "int32").set("_value", "1").toString().getBytes(UTF_8);
   }

   private byte[] valueAsJson() {
      Json parent = Json.object()
            .set("_type", "Parent")
            .set("id", 1)
            .set("name", "Kim")
            .set("slowChildren", Json.array(Json.object().set("id", "0")))
            .set("fastChildren", Json.array(Json.object().set("id", "10")))
            .set("fieldLessChildren", Json.array(Json.object().set("id", "0")));
      return parent.toString().getBytes(UTF_8);
   }

   private void registerProtoBuf() throws Exception {
      RemoteCache<String, String> protoCache = remoteCacheManager.getCache(PROTOBUF_METADATA_CACHE_NAME);
      String protobuf = Util.getResourceAsString(SCHEMA_FILE, getClass().getClassLoader());
      protoCache.put(SCHEMA_FILE, protobuf);
   }

   private XMLStringConfiguration createCacheXMLConfig() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.encoding().mediaType(APPLICATION_PROTOSTREAM_TYPE);
      builder.indexing().enable().storage(LOCAL_HEAP).addIndexedEntities("Parent");
      String config = builder.build().toXMLString(CACHE_NAME);
      return new XMLStringConfiguration(config);
   }

}
