package org.infinispan.factories.threads;

import org.infinispan.commons.ThreadGroups;
import org.infinispan.commons.executors.BlockingResource;

public class BlockingThreadFactory extends DefaultThreadFactory implements BlockingResource {

   public BlockingThreadFactory(int initialPriority, String threadNamePattern, String node, String component) {
      super(ThreadGroups.BLOCKING_GROUP, initialPriority, threadNamePattern, node, component);
   }

   public BlockingThreadFactory(String name, String threadGroupName, int initialPriority,
         String threadNamePattern, String node, String component) {
      super(name, new ThreadGroups.ISPNBlockingThreadGroup(threadGroupName), initialPriority, threadNamePattern, node, component);
   }

}
