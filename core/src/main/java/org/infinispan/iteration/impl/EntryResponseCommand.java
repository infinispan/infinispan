package org.infinispan.iteration.impl;

import org.infinispan.commands.TopologyAffectedCommand;
import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.remoting.transport.Address;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Command sent to respond with entry values for given segments
 *
 * @author wburns
 * @since 7.0
 */
public class EntryResponseCommand<K, C> extends BaseRpcCommand implements TopologyAffectedCommand {
   public static final byte COMMAND_ID = 42;

   private UUID identifier;
   private Set<Integer> completedSegments;
   private Set<Integer> inDoubtSegments;
   private Collection<CacheEntry<K, C>> values;
   private int topologyId = -1;
   private Address origin;

   private EntryRetriever<K, ?> entryRetrieverManager;

   // Only here for CommandIdUniquenessTest
   private EntryResponseCommand() {
      super(null);
   }

   public EntryResponseCommand(String cacheName) {
      super(cacheName);
   }

   public EntryResponseCommand(Address origin, String cacheName, UUID identifier, Set<Integer> completedSegments,
                               Set<Integer> inDoubtSegments, Collection<CacheEntry<K, C>> values) {
      super(cacheName);
      this.origin = origin;
      this.identifier = identifier;
      this.completedSegments = completedSegments;
      this.inDoubtSegments = inDoubtSegments;
      this.values = values;

   }

   @Inject
   public void init(EntryRetriever<K, ?> entryRetrieverManager) {
      this.entryRetrieverManager = entryRetrieverManager;
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      entryRetrieverManager.receiveResponse(identifier, origin, completedSegments, inDoubtSegments, values);
      return null;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public Object[] getParameters() {
      return new Object[]{origin, identifier, completedSegments, inDoubtSegments, values, topologyId};
   }

   @Override
   public void setParameters(int commandId, Object[] parameters) {
      int i = 0;
      origin = (Address) parameters[i++];
      identifier = (UUID) parameters[i++];
      completedSegments = (Set<Integer>) parameters[i++];
      inDoubtSegments = (Set<Integer>) parameters[i++];
      values = (Collection<CacheEntry<K, C>>)parameters[i++];
      topologyId = (Integer) parameters[i++];
   }

   @Override
   public boolean isReturnValueExpected() {
      return false;
   }

   @Override
   public int getTopologyId() {
      return topologyId;
   }

   @Override
   public void setTopologyId(int topologyId) {
      this.topologyId = topologyId;
   }

   @Override
   public String toString() {
      return "EntryResponseCommand{" +
            "identifier=" + identifier +
            ", completedSegments=" + completedSegments +
            ", inDoubtSegments=" + inDoubtSegments +
            ", values=" + values +
            ", topologyId=" + topologyId +
            ", origin=" + origin +
            '}';
   }
}
