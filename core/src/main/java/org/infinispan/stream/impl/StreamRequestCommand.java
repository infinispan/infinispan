package org.infinispan.stream.impl;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import org.infinispan.commands.TopologyAffectedCommand;
import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.commons.util.Util;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.ByteString;
import org.infinispan.util.concurrent.CompletableFutures;

/**
 * Stream request command that is sent to remote nodes handle execution of remote intermediate and terminal operations.
 * @param <K> the key type
 */
public class StreamRequestCommand<K> extends BaseRpcCommand implements TopologyAffectedCommand {
   public static final byte COMMAND_ID = 47;

   private LocalStreamManager lsm;

   private Object id;
   private Type type;
   private boolean parallelStream;
   private Set<Integer> segments;
   private Set<K> keys;
   private Set<K> excludedKeys;
   private boolean includeLoader;
   private Object terminalOperation;
   private int topologyId = -1;

   @Override
   public int getTopologyId() {
      return topologyId;
   }

   @Override
   public void setTopologyId(int topologyId) {
      this.topologyId = topologyId;
   }

   public Object getId() {
      return id;
   }

   public enum Type {
      TERMINAL,
      TERMINAL_REHASH,
      TERMINAL_KEY,
      TERMINAL_KEY_REHASH;

      private static final Type[] CACHED_VALUES = values();
   }

   // Only here for CommandIdUniquenessTest
   private StreamRequestCommand() { super(null); }

   public StreamRequestCommand(ByteString cacheName) {
      super(cacheName);
   }

   public StreamRequestCommand(ByteString cacheName, Address origin, Object id, boolean parallelStream, Type type,
                               Set<Integer> segments, Set<K> keys, Set<K> excludedKeys, boolean includeLoader,
                               Object terminalOperation) {
      super(cacheName);
      setOrigin(origin);
      this.id = id;
      this.parallelStream = parallelStream;
      this.type = type;
      this.segments = segments;
      this.keys = keys;
      this.excludedKeys = excludedKeys;
      this.includeLoader = includeLoader;
      this.terminalOperation = terminalOperation;
   }

   @Inject
   public void inject(LocalStreamManager lsm) {
      this.lsm = lsm;
   }

   @Override
   public CompletableFuture<Object> invokeAsync() throws Throwable {
      switch (type) {
         case TERMINAL:
            lsm.streamOperation(id, getOrigin(), parallelStream, segments, keys, excludedKeys, includeLoader,
                    (TerminalOperation) terminalOperation);
            break;
         case TERMINAL_REHASH:
            lsm.streamOperationRehashAware(id, getOrigin(), parallelStream, segments, keys, excludedKeys, includeLoader,
                    (TerminalOperation) terminalOperation);
            break;
         case TERMINAL_KEY:
            lsm.streamOperation(id, getOrigin(), parallelStream, segments, keys, excludedKeys, includeLoader,
                    (KeyTrackingTerminalOperation) terminalOperation);
            break;
         case TERMINAL_KEY_REHASH:
            lsm.streamOperationRehashAware(id, getOrigin(), parallelStream, segments, keys, excludedKeys, includeLoader,
                    (KeyTrackingTerminalOperation) terminalOperation);
            break;
      }
      return CompletableFutures.completedNull();
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      output.writeObject(getOrigin());
      output.writeObject(id);
      output.writeBoolean(parallelStream);
      MarshallUtil.marshallEnum(type, output);
      MarshallUtil.marshallCollection(segments, output);
      MarshallUtil.marshallCollection(keys, output);
      MarshallUtil.marshallCollection(excludedKeys, output);
      output.writeBoolean(includeLoader);
      output.writeObject(terminalOperation);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      setOrigin((Address) input.readObject());
      id = input.readObject();
      parallelStream = input.readBoolean();
      type = MarshallUtil.unmarshallEnum(input, ordinal -> Type.CACHED_VALUES[ordinal]);
      segments = MarshallUtil.unmarshallCollectionUnbounded(input, HashSet::new);
      keys = MarshallUtil.unmarshallCollectionUnbounded(input, HashSet::new);
      excludedKeys = MarshallUtil.unmarshallCollectionUnbounded(input, HashSet::new);
      includeLoader = input.readBoolean();
      terminalOperation = input.readObject();
   }

   @Override
   public boolean isReturnValueExpected() {
      return false;
   }

   @Override
   public boolean canBlock() {
      return true;
   }

   @Override
   public String toString() {
      final StringBuilder sb = new StringBuilder("StreamRequestCommand{");
      sb.append("type=").append(type);
      sb.append(", includeLoader=").append(includeLoader);
      sb.append(", terminalOperation=").append(terminalOperation);
      sb.append(", topologyId=").append(topologyId);
      sb.append(", id=").append(id);
      sb.append(", segments=").append(segments);
      sb.append(", keys=").append(Util.toStr(keys));
      sb.append(", excludedKeys=").append(Util.toStr(excludedKeys));
      sb.append('}');
      return sb.toString();
   }
}
