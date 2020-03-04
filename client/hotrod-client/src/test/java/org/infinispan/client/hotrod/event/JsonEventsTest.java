package org.infinispan.client.hotrod.event;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.withClientListener;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON;
import static org.infinispan.query.remote.client.ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME;

import org.infinispan.client.hotrod.DataFormat;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.query.RemoteQueryTestUtils;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.commons.marshall.UTF8StringMarshaller;
import org.testng.annotations.Test;

/**
 * @since 9.4
 */
@Test(groups = {"functional",}, testName = "client.hotrod.event.JsonEventsTest")
public class JsonEventsTest extends SingleHotRodServerTest {

   @Override
   protected void setup() throws Exception {
      super.setup();
      RemoteCache<String, String> schemaCache = remoteCacheManager.getCache(PROTOBUF_METADATA_CACHE_NAME);
      schemaCache.put("schema.proto", "message A { optional string key = 1; }");
      RemoteQueryTestUtils.checkSchemaErrors(schemaCache);
   }

   public void testCreatedEvent() {
      DataFormat jsonValues = DataFormat.builder().valueType(APPLICATION_JSON).valueMarshaller(new UTF8StringMarshaller()).build();
      final EventLogListener<Integer> l = new EventLogListener<>(remoteCacheManager.getCache().withDataFormat(jsonValues));
      withClientListener(l, remote -> {
         l.expectNoEvents();
         remote.put(1, "{\"_type\":\"A\",\"key\":\"one\"}");
         l.expectOnlyCreatedEvent(1);
         remote.put(2, "{\"_type\":\"A\",\"key\":\"two\"}");
         l.expectOnlyCreatedEvent(2);
      });
   }
}
