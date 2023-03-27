package org.infinispan.commons.jdk21;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 11.0
 **/
public class ThreadCreatorImpl implements org.infinispan.commons.spi.ThreadCreator {

   public Thread createThread(ThreadGroup group, Runnable runnable, boolean lightweight) {
      if (lightweight) {
         return Thread.ofVirtual().unstarted(runnable);
      } else {
         return new Thread(group, runnable);
      }
   }

}
