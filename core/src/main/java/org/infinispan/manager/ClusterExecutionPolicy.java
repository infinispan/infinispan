package org.infinispan.manager;

import org.infinispan.remoting.transport.TopologyAwareAddress;

/**
 * ClusterExecutionPolicy controls how which nodes commands submitted via {@link ClusterExecutor}. That is the user
 * can ensure that a command goes or doesn't go to a specific physical location such as on the existing machine, rack
 * or site.
 * <p>
 * ClusterExecutionPolicy effectively scopes execution of commands to a subset of nodes. For
 * example, someone might want to exclusively execute commands on a local network site instead of a
 * backup remote network centre as well. Others might, for example, use only a dedicated subset of a
 * certain Infinispan rack nodes for specific task execution.
 *
 *
 * @author William Burns
 * @since 9.0
 */
public enum ClusterExecutionPolicy {
   /**
    * The command can be executed on any node in the cluster
    */
   ALL {
      @Override
      public boolean include(TopologyAwareAddress thisAddress, TopologyAwareAddress otherAddress) {
         return true;
      }
   },
   /**
    * The command can be executed only on the same machine from where it was initiated.  Note this implies
    * same rack and same site.
    */
   SAME_MACHINE {
      @Override
      public boolean include(TopologyAwareAddress thisAddress, TopologyAwareAddress otherAddress) {
         return thisAddress.isSameMachine(otherAddress);
      }
   },
   /**
    * The command will be executed only on a different machine.  Note this means it may or may not be on the same rack
    * or site.
    */
   DIFFERENT_MACHINE {
      @Override
      public boolean include(TopologyAwareAddress thisAddress, TopologyAwareAddress otherAddress) {
         return !thisAddress.isSameMachine(otherAddress);
      }
   },
   /**
    * The command will be executed on a machine on the same rack.  Note this means it may or may not be executed on the
    * same machine.
    */
   SAME_RACK {
      @Override
      public boolean include(TopologyAwareAddress thisAddress, TopologyAwareAddress otherAddress) {
         return thisAddress.isSameRack(otherAddress);
      }
   },
   /**
    * The command will be executed on machine on a different rack.  Note this means may or may not be on the same site.
    */
   DIFFERENT_RACK {
      @Override
      public boolean include(TopologyAwareAddress thisAddress, TopologyAwareAddress otherAddress) {
         return !thisAddress.isSameRack(otherAddress);
      }
   },
   /**
    * The command will be executed on a machine on the same site.  Note this means it may or may not be executed on the
    * same machine or even same rack.
    */
   SAME_SITE {
      @Override
      public boolean include(TopologyAwareAddress thisAddress, TopologyAwareAddress otherAddress) {
         return thisAddress.isSameSite(otherAddress);
      }
   },
   /**
    * The command will be executed on a different site.
    */
   DIFFERENT_SITE {
      @Override
      public boolean include(TopologyAwareAddress thisAddress, TopologyAwareAddress otherAddress) {
         return !thisAddress.isSameSite(otherAddress);
      }
   },
   ;

   public abstract boolean include(TopologyAwareAddress thisAddress, TopologyAwareAddress otherAddress);
}
