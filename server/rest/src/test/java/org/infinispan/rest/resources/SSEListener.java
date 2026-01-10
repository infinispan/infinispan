package org.infinispan.rest.resources;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertNotNull;
import static org.testng.AssertJUnit.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.infinispan.client.rest.RestEventListener;
import org.infinispan.client.rest.RestResponseInfo;
import org.infinispan.util.KeyValuePair;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 13.0
 **/
public class SSEListener implements RestEventListener {
   protected static final Consumer<KeyValuePair<String, String>> NO_OP = ignore -> {};
   private static final Log log = LogFactory.getLog(SSEListener.class);

   BlockingDeque<KeyValuePair<String, String>> events = new LinkedBlockingDeque<>();
   private final CountDownLatch openLatch;

   public SSEListener() {
      this.openLatch = new CountDownLatch(1);
   }


   @Override
   public void onOpen(RestResponseInfo response) {
      log.tracef("open");
      if (response.status() < 300) {
         openLatch.countDown();
      }
   }

   @Override
   public void onMessage(String id, String type, String data) {
      log.tracef("Received %s %s %s", id, type, data);
      this.events.add(new KeyValuePair<>(type, data));
   }

   public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
      return openLatch.await(timeout, unit);
   }

   public void expectEvent(String type, String subString) throws InterruptedException {
      expectEvent(type, subString, NO_OP);
   }

   public List<KeyValuePair<String, String>> poll(int num) throws InterruptedException {
      List<KeyValuePair<String, String>> polled = new ArrayList<>();

      for (int i = 0; i < num; i++) {
         KeyValuePair<String, String> event = events.poll(10, TimeUnit.SECONDS);
         assertNotNull(event);
         polled.add(event);
      }
      return polled;
   }

   public void expectEvent(String type, String subString, Consumer<KeyValuePair<String, String>> consumer) throws InterruptedException {
      KeyValuePair<String, String> event = events.poll(10, TimeUnit.SECONDS);
      assertNotNull(event);
      assertEquals(type, event.getKey());
      assertThat(event.getValue()).contains(subString);
      consumer.accept(event);
   }
}
