package org.infinispan.factories.threads;

import org.infinispan.commons.executors.BlockingResource;

public class BlockingThreadFactory extends DefaultThreadFactory implements BlockingResource {
   public BlockingThreadFactory(String threadGroupName, int initialPriority, String threadNamePattern,
         String node, String component) {
      super(ISPNBlockingThreadGroup.GROUP, initialPriority, threadNamePattern, node, component);
   }

   public BlockingThreadFactory(String name, String threadGroupName, int initialPriority,
         String threadNamePattern, String node, String component) {
      super(name, ISPNBlockingThreadGroup.GROUP, initialPriority, threadNamePattern, node, component);
   }

   public static final class ISPNBlockingThreadGroup extends ThreadGroup implements BlockingResource {
      public ISPNBlockingThreadGroup(String name) {
         super(name);
      }

      private static final ThreadGroup GROUP = new BlockingThreadFactory.ISPNBlockingThreadGroup("ISPN-blocking-group");
   }
}
