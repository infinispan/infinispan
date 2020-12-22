package org.infinispan.rest.resources;

import java.util.concurrent.BlockingDeque;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;

import org.infinispan.client.rest.RestEventListener;
import org.infinispan.client.rest.RestResponse;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 13.0
 **/
public class SSEListener implements RestEventListener {
   BlockingDeque<String> events = new LinkedBlockingDeque<>();
   BlockingDeque<String> data = new LinkedBlockingDeque<>();
   CountDownLatch openLatch = new CountDownLatch(1);

   @Override
   public void onOpen(RestResponse response) {
      openLatch.countDown();
   }

   @Override
   public void onMessage(String id, String type, String data) {
      this.events.add(type);
      this.data.add(data);
   }
}
