package org.infinispan.factories.threads;

import org.infinispan.commons.ThreadGroups;
import org.infinispan.commons.executors.NonBlockingResource;

public class NonBlockingThreadFactory extends DefaultThreadFactory implements NonBlockingResource {

   public NonBlockingThreadFactory(int initialPriority, String threadNamePattern, String node, String component) {
      super(ThreadGroups.NON_BLOCKING_GROUP, initialPriority, threadNamePattern, node, component);
   }

   public NonBlockingThreadFactory(String name, String threadGroupName, int initialPriority,
                                          String threadNamePattern, String node, String component) {
      super(name, new ThreadGroups.ISPNNonBlockingThreadGroup(threadGroupName), initialPriority, threadNamePattern, node, component);
   }

}
