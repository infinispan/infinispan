package org.infinispan.client.rest.impl.jdk;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @since 15.0
 **/
public class RestClientThreadFactory implements ThreadFactory {
   private final AtomicInteger nextId = new AtomicInteger();
   private final String namePrefix;

   public RestClientThreadFactory(long id) {
      namePrefix = "HttpClient-" + id + "-Worker-";
   }

   @Override
   public Thread newThread(Runnable r) {
      String name = namePrefix + nextId.getAndIncrement();
      Thread t = new Thread(null, r, name, 0, false);
      t.setDaemon(true);
      return t;
   }
}
