package org.infinispan.iteration.impl;

import org.infinispan.commands.TopologyAffectedCommand;
import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.filter.Converter;
import org.infinispan.filter.KeyValueFilter;
import org.infinispan.remoting.transport.Address;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashSet;
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
   private Set<K> keysToFilter;
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

   public EntryRequestCommand(String cacheName, UUID identifier, Address origin, Set<Integer> segments, Set<K> keysToFilter,
                              KeyValueFilter<? super K, ? super V> filter, Converter<? super K, ? super V, C> converter,
                              Set<Flag> flags) {
      super(cacheName);
      setOrigin(origin);
      this.identifier = identifier;
      this.segments = segments;
      this.keysToFilter = keysToFilter;
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
      entryRetrieverManager.startRetrievingValues(identifier, getOrigin(), segments, keysToFilter, filter, converter, flags);
      return null;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      MarshallUtil.marshallUUID(identifier, output, false);
      output.writeObject(getOrigin());
      MarshallUtil.marshallCollection(segments, output);
      MarshallUtil.marshallCollection(keysToFilter, output);
      output.writeObject(filter);
      output.writeObject(converter);
      output.writeInt(topologyId);
      output.writeObject(Flag.copyWithoutRemotableFlags(flags));
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      identifier = MarshallUtil.unmarshallUUID(input, false);
      setOrigin((Address) input.readObject());
      segments = MarshallUtil.unmarshallCollectionUnbounded(input, HashSet::new);
      keysToFilter = MarshallUtil.unmarshallCollectionUnbounded(input, HashSet::new);
      filter = (KeyValueFilter<? super K, ? super V>) input.readObject();
      converter = (Converter<? super K, ? super V, C>) input.readObject();
      topologyId = input.readInt();
      flags = (Set<Flag>) input.readObject();
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
            ", keysToFilter=" + keysToFilter +
            ", filter=" + filter +
            ", converter=" + converter +
            ", topologyId=" + topologyId +
            ", flags=" + flags +
            '}';
   }
}
