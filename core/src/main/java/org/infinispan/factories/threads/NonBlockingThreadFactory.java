package org.infinispan.factories.threads;

import org.infinispan.commons.executors.NonBlockingResource;

public class NonBlockingThreadFactory extends DefaultThreadFactory implements NonBlockingResource {
   public NonBlockingThreadFactory(String threadGroupName, int initialPriority, String threadNamePattern,
                                          String node, String component) {
      super(new ISPNNonBlockingThreadGroup(threadGroupName), initialPriority, threadNamePattern, node, component);
   }

   public NonBlockingThreadFactory(String name, String threadGroupName, int initialPriority,
                                          String threadNamePattern, String node, String component) {
      super(name, new ISPNNonBlockingThreadGroup(threadGroupName), initialPriority, threadNamePattern, node, component);
   }

   public static final class ISPNNonBlockingThreadGroup extends ThreadGroup implements NonBlockingResource {
      public ISPNNonBlockingThreadGroup(String name) {
         super(name);
      }
   }
}
