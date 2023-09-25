package org.infinispan.factories.threads;

import org.infinispan.commons.executors.NonBlockingResource;

public class NonBlockingThreadFactory extends DefaultThreadFactory implements NonBlockingResource {
   public NonBlockingThreadFactory(String threadGroupName, int initialPriority, String threadNamePattern,
                                          String node, String component) {
      super(ISPNNonBlockingThreadGroup.GROUP, initialPriority, threadNamePattern, node, component);
   }

   public NonBlockingThreadFactory(String name, String threadGroupName, int initialPriority,
                                          String threadNamePattern, String node, String component) {
      super(name, ISPNNonBlockingThreadGroup.GROUP, initialPriority, threadNamePattern, node, component);
   }

   static final class ISPNNonBlockingThreadGroup extends ThreadGroup implements NonBlockingResource {
      ISPNNonBlockingThreadGroup(String name) {
         super(name);
      }

      private static final ThreadGroup GROUP = new NonBlockingThreadFactory.ISPNNonBlockingThreadGroup("ISPN-non-blocking-group");
   }
}
