package org.infinispan.commons.jdkspecific;

import org.infinispan.commons.logging.Log;
import org.infinispan.commons.util.Util;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 11.0
 **/
public class ThreadCreator {
   static org.infinispan.commons.spi.ThreadCreator INSTANCE = getInstance();

   private static org.infinispan.commons.spi.ThreadCreator getInstance() {
      try {
         if (Boolean.getBoolean("org.infinispan.threads.virtual")) {
            org.infinispan.commons.spi.ThreadCreator instance = Util.getInstance("org.infinispan.commons.jdk21.ThreadCreatorImpl", ThreadCreator.class.getClassLoader());
            Log.CONTAINER.infof("Virtual threads support enabled");
            return instance;
         }
      } catch (Throwable t) {
         Log.CONTAINER.debugf("Could not initialize virtual threads", t);
      }
      return new ThreadCreatorImpl();
   }


   public static Thread createThread(ThreadGroup threadGroup, Runnable target, boolean lightweight) {
      return INSTANCE.createThread(threadGroup, target, lightweight);
   }

   static class ThreadCreatorImpl implements org.infinispan.commons.spi.ThreadCreator {

      @Override
      public Thread createThread(ThreadGroup threadGroup, Runnable target, boolean lightweight) {
         return new Thread(threadGroup, target);
      }
   }
}
