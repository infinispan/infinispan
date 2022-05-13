package org.infinispan.rest.resources;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.assertEquals;

import java.util.concurrent.BlockingDeque;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.infinispan.client.rest.RestEventListener;
import org.infinispan.client.rest.RestResponse;
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
   CountDownLatch openLatch = new CountDownLatch(1);

   @Override
   public void onOpen(RestResponse response) {
      log.tracef("open");
      openLatch.countDown();
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

   public void expectEvent(String type, String subString, Consumer<KeyValuePair<String, String>> consumer) throws InterruptedException {
      KeyValuePair<String, String> event = events.poll(10, TimeUnit.SECONDS);
      assertNotNull(event);
      assertEquals(type, event.getKey());
      assertTrue(event.getValue().contains(subString));
      consumer.accept(event);
   }
}
