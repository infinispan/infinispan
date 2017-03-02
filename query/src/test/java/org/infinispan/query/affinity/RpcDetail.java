package org.infinispan.query.affinity;

import java.util.Collection;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.remoting.transport.Address;


class RpcDetail {

   private final String cacheName;
   private final ReplicableCommand command;
   private final Collection<Address> destination;
   private final Address originator;

   RpcDetail(Address originator, ReplicableCommand command, String cacheName, Collection<Address> destination) {
      this.originator = originator;
      this.command = command;
      this.cacheName = cacheName;
      this.destination = destination;
   }

   ReplicableCommand getCommand() {
      return command;
   }

   String getCacheName() {
      return cacheName;
   }

   boolean isRpcToItself() {
      return destination != null && destination.size() == 1 && destination.iterator().next().equals(originator);
   }

   @Override
   public String toString() {
      return "RpcDetail{" +
            "cacheName='" + cacheName + '\'' +
            ", command=" + command +
            ", destination=" + destination +
            ", originator=" + originator +
            '}';
   }
}
