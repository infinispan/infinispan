package org.infinispan.server.websocket.handlers;

import org.infinispan.server.websocket.json.JsonObject;
import org.infinispan.websocket.MockChannel;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import static org.infinispan.assertions.JsonPayloadAssertion.assertThat;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.fail;

/**
 * Tests Operation handlers.
 *
 * @author <a href="mailto:tom.fennelly@gmail.com">tom.fennelly@gmail.com</a>
 */
@Test (testName = "websocket.handlers.OpHandlerTest", groups = "unit")
public class OpHandlerTest {

   public static final String CACHE_NAME = "cacheName";

   private MockChannel serverChannel;
   private MockClient cacheClient;

   @BeforeTest
   public void beforeTest() {
      serverChannel = new MockChannel();
      cacheClient = new MockClient(CACHE_NAME, serverChannel);
   }

   @AfterTest
   public void afterTest() {
      if(cacheClient != null) {
         cacheClient.stop();
      }

      if(serverChannel != null) {
         serverChannel.clear();
      }
   }

   public void shouldReturnPreviouslyPutValueInJsonPayload() throws Exception {
      //when
      cacheClient.put("a", "aVal");
      cacheClient.get("a");
      JsonObject payload = serverChannel.getJSONPayload();

      //then
      assertThat(payload).hasCacheName(CACHE_NAME).hasKey("a").hasValue("aVal").hasMimeType("text/plain");
   }

   public void shouldReturnNullWhenValueIsNotInCache() throws Exception {
      //when
      cacheClient.get("notInCache");
      JsonObject payload = serverChannel.getJSONPayload();

      //then
      assertThat(payload).hasCacheName(CACHE_NAME).hasKey("notInCache").hasValue(null);
   }

   public void shouldReturnPreviouslyPutValueOnNotify() throws Exception {
      //given
      cacheClient.put("a", "aVal");

      //when
      cacheClient.notify("a");
      JsonObject payload = serverChannel.getJSONPayload(1000);

      //then
      assertThat(payload).hasKey("a").hasValue("aVal");
   }

   public void shouldCallNotifyIfModifyingValueInCache() throws Exception {
      //given
      cacheClient.put("a", "oldValue");
      cacheClient.notify("a");
      serverChannel.getJSONPayload(1000);

      //when
      cacheClient.getCache().put("a", "newValue");
      JsonObject payload = serverChannel.getJSONPayload();

      //then
      assertThat(payload).hasKey("a").hasValue("newValue");
   }

   public void shouldNotCallNotifyWhenListeningToDataWithoutNotifications() throws Exception {
      //given
      cacheClient.put("notificationKey", "aVal");
      cacheClient.put("irrelevantKey", "bVal");
      cacheClient.notify("notificationKey");
      serverChannel.getJSONPayload();

      //when
      cacheClient.getCache().put("irrelevantKey", "newValue");
      try {
         JsonObject jsonPayload = serverChannel.getJSONPayload(250);
         fail("Expected timeout" + jsonPayload);
      } catch (RuntimeException e) {
         assertEquals(e.getMessage(), "Timed out waiting for data to be pushed onto the channel.");
      }
   }

   /**
    * This test is a bit tricky. It registers notifications to all channels, so if executed in parallel it might
    * crash some other tests. This is why we need to add dependency on particular methods - we need to make sure that
    * this is test is executed last.
    */
   @Test(dependsOnMethods = {"shouldNotCallNotifyWhenListeningToDataWithoutNotifications",
         "shouldCallNotifyIfModifyingValueInCache"})
   public void shouldCallNotifyWhenListeningToAllNotifications() throws Exception {
      //given
      cacheClient.put("a", "aVal");
      cacheClient.put("b", "bVal");
      cacheClient.notify("*");
      serverChannel.getJSONPayload();

      //when
      cacheClient.getCache().put("b", "newValue");
      JsonObject payload = serverChannel.getJSONPayload();

      //then
      assertThat(payload).hasKey("b").hasValue("newValue");
   }

   public void shouldReturnPreviouslyPutValueOnRemove() throws Exception {
      //given
      cacheClient.put("a", "aVal");
      cacheClient.get("a");
      serverChannel.getJSONPayload();

      //when
      cacheClient.remove("a");
      cacheClient.get("a");
      JsonObject payload = serverChannel.getJSONPayload();

      //then
      assertThat(payload).hasCacheName(CACHE_NAME).hasKey("a").hasValue(null);
   }
}
