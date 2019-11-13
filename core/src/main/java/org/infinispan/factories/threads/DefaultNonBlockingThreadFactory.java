package org.infinispan.factories.threads;

import org.infinispan.commons.executors.NonBlockingThread;
import org.infinispan.commons.executors.NonBlockingThreadFactory;

public class DefaultNonBlockingThreadFactory extends DefaultThreadFactory implements NonBlockingThreadFactory {
   public DefaultNonBlockingThreadFactory(ThreadGroup threadGroup, int initialPriority, String threadNamePattern,
                                          String node, String component) {
      super(threadGroup, initialPriority, threadNamePattern, node, component);
   }

   public DefaultNonBlockingThreadFactory(String name, ThreadGroup threadGroup, int initialPriority,
                                          String threadNamePattern, String node, String component) {
      super(name, threadGroup, initialPriority, threadNamePattern, node, component);
   }

   @Override
   protected Thread actualThreadCreate(ThreadGroup threadGroup, Runnable target) {
      return new ISPNNonBlockingThread(threadGroup, target);
   }

   static final class ISPNNonBlockingThread extends Thread implements NonBlockingThread {
      ISPNNonBlockingThread(ThreadGroup threadGroup, Runnable target) {
         super(threadGroup, target);
      }
   }
}
