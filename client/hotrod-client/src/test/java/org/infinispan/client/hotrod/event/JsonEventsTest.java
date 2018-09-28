package org.infinispan.client.hotrod.event;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.withClientListener;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON;

import org.infinispan.client.hotrod.DataFormat;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.commons.marshall.UTF8StringMarshaller;
import org.testng.annotations.Test;

/**
 * @since 9.4
 */
@Test(groups = {"functional",}, testName = "client.hotrod.event.JsonEventsTest")
public class JsonEventsTest extends SingleHotRodServerTest {

   public void testCreatedEvent() {
      DataFormat jsonValues = DataFormat.builder().valueType(APPLICATION_JSON).valueMarshaller(new UTF8StringMarshaller()).build();
      final EventLogListener<Integer> l = new EventLogListener<>(remoteCacheManager.getCache().withDataFormat(jsonValues));
      withClientListener(l, remote -> {
         l.expectNoEvents();
         remote.put(1, "{\"key\":\"one\"}");
         l.expectOnlyCreatedEvent(1);
         remote.put(2, "{\"key\":\"two\"}");
         l.expectOnlyCreatedEvent(2);
      });
   }
}
