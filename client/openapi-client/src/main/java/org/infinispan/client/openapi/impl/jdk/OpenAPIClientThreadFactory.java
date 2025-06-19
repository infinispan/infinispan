package org.infinispan.client.openapi.impl.jdk;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @since 15.0
 **/
public class OpenAPIClientThreadFactory implements ThreadFactory {
   private final AtomicInteger nextId = new AtomicInteger();
   private final String namePrefix;

   public OpenAPIClientThreadFactory(long id) {
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
