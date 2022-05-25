package org.infinispan.remoting.transport.jgroups;

import org.infinispan.commons.util.Util;

/**
 * Utility methods for jgroups-raft
 *
 * @since 14.0
 */
public enum RaftUtil {
   ;
   private static final boolean RAFT_IN_CLASSPATH;
   private static final String RAFT_CLASS = "org.jgroups.protocols.raft.RAFT";

   static {
      boolean raftFound = true;
      try {
         Util.loadClassStrict(RAFT_CLASS, JGroupsRaftManager.class.getClassLoader());
      } catch (ClassNotFoundException e) {
         raftFound = false;
      }
      RAFT_IN_CLASSPATH = raftFound;
   }

   public static boolean isRaftAvailable() {
      return RAFT_IN_CLASSPATH;
   }
}
