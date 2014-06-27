package org.infinispan.iteration.impl;

import org.infinispan.commands.TopologyAffectedCommand;
import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.filter.Converter;
import org.infinispan.filter.KeyValueFilter;
import org.infinispan.remoting.transport.Address;

import java.util.Set;
import java.util.UUID;

/**
 * Command sent to request entry iterator values for given segments.
 *
 * @author wburns
 * @since 7.0
 */
public class EntryRequestCommand<K, V, C> extends BaseRpcCommand implements TopologyAffectedCommand {
   public static final byte COMMAND_ID = 41;

   private UUID identifier;
   private Set<Integer> segments;
   private KeyValueFilter<? super K, ? super V> filter;
   private Converter<? super K, ? super V, C> converter;
   private Set<Flag> flags;
   private int topologyId = -1;

   private EntryRetriever<K, V> entryRetrieverManager;

   // Only here for CommandIdUniquenessTest
   private EntryRequestCommand() {
      super(null);
   }

   public EntryRequestCommand(String cacheName) {
      super(cacheName);
   }

   public EntryRequestCommand(String cacheName, UUID identifier, Address origin, Set<Integer> segments,
                              KeyValueFilter<? super K, ? super V> filter, Converter<? super K, ? super V, C> converter,
                              Set<Flag> flags) {
      super(cacheName);
      setOrigin(origin);
      this.identifier = identifier;
      this.segments = segments;
      this.filter = filter;
      this.converter = converter;
      this.flags = flags;
   }

   @Inject
   public void init(EntryRetriever<K, V> entryRetrieverManager) {
      this.entryRetrieverManager = entryRetrieverManager;
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      entryRetrieverManager.startRetrievingValues(identifier, getOrigin(), segments, filter, converter, flags);
      return null;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public Object[] getParameters() {
      return new Object[]{identifier, getOrigin(), segments, filter, converter, topologyId, flags};
   }

   @Override
   public void setParameters(int commandId, Object[] parameters) {
      int i = 0;
      identifier = (UUID) parameters[i++];
      setOrigin((Address) parameters[i++]);
      segments = (Set<Integer>) parameters[i++];
      filter = (KeyValueFilter<? super K, ? super V>) parameters[i++];
      converter = (Converter<? super K, ? super V, C>) parameters[i++];
      topologyId = (Integer) parameters[i++];
      flags = (Set<Flag>)parameters[i++];
   }

   @Override
   public boolean isReturnValueExpected() {
      return true;
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
      return "EntryRequestCommand{" +
            "identifier=" + identifier +
            ", segments=" + segments +
            ", filter=" + filter +
            ", converter=" + converter +
            ", topologyId=" + topologyId +
            ", flags=" + flags +
            '}';
   }
}
