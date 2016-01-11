package org.infinispan.iteration.impl;

import org.infinispan.commands.TopologyAffectedCommand;
import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.remoting.transport.Address;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
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
   private CacheException e;
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
                               Set<Integer> inDoubtSegments, Collection<CacheEntry<K, C>> values, CacheException e) {
      super(cacheName);
      this.origin = origin;
      this.identifier = identifier;
      this.completedSegments = completedSegments;
      this.inDoubtSegments = inDoubtSegments;
      this.values = values;
      this.e = e;

   }

   @Inject
   public void init(EntryRetriever<K, ?> entryRetrieverManager) {
      this.entryRetrieverManager = entryRetrieverManager;
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      entryRetrieverManager.receiveResponse(identifier, origin, completedSegments, inDoubtSegments, values, e);
      return null;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      output.writeObject(origin);
      MarshallUtil.marshallUUID(identifier, output, false);
      MarshallUtil.marshallCollection(completedSegments, output);
      MarshallUtil.marshallCollection(inDoubtSegments, output);
      MarshallUtil.marshallCollection(values, output);
      output.writeObject(e);
      output.writeInt(topologyId);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      origin = (Address) input.readObject();
      identifier = MarshallUtil.unmarshallUUID(input, false);
      completedSegments = MarshallUtil.unmarshallCollectionUnbounded(input, HashSet::new);
      inDoubtSegments = MarshallUtil.unmarshallCollectionUnbounded(input, HashSet::new);
      values = MarshallUtil.unmarshallCollection(input, ArrayList::new);
      e = (CacheException) input.readObject();
      topologyId = input.readInt();
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
