package org.infinispan.stream.impl;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.commons.marshall.UserObjectInput;
import org.infinispan.commons.util.IntSet;
import org.infinispan.marshall.core.MarshalledEntryFactory;
import org.infinispan.commons.marshall.UserObjectOutput;
import org.infinispan.remoting.transport.Address;
import org.infinispan.stream.impl.intops.IntermediateOperation;
import org.infinispan.util.ByteString;

/**
 * Stream request command that is sent to remote nodes handle execution of remote intermediate and terminal operations.
 * @param <K> the key type
 */
public class StreamIteratorRequestCommand<K> extends StreamIteratorNextCommand {
   public static final byte COMMAND_ID = 70;

   private boolean parallelStream;
   private IntSet segments;
   private Set<K> keys;
   private Set<K> excludedKeys;
   private boolean includeLoader;
   private boolean entryStream;
   private Iterable<IntermediateOperation> intOps;

   // Only here for CommandIdUniquenessTest
   private StreamIteratorRequestCommand() { super(null); }

   public StreamIteratorRequestCommand(ByteString cacheName) {
      super(cacheName);
   }

   public StreamIteratorRequestCommand(ByteString cacheName, Address origin, Object id, boolean parallelStream,
         IntSet segments, Set<K> keys, Set<K> excludedKeys, boolean includeLoader, boolean entryStream,
                               Iterable<IntermediateOperation> intOps, long batchSize) {
      super(cacheName, id, batchSize);
      setOrigin(origin);
      this.parallelStream = parallelStream;
      this.segments = segments;
      this.keys = keys;
      this.excludedKeys = excludedKeys;
      this.includeLoader = includeLoader;
      this.entryStream = entryStream;
      this.intOps = intOps;
   }

   @Override
   public CompletableFuture<Object> invokeAsync() throws Throwable {
      return CompletableFuture.completedFuture(lsm.startIterator(id, getOrigin(), segments, keys, excludedKeys,
            includeLoader, entryStream, intOps, batchSize));
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public void writeTo(UserObjectOutput output, MarshalledEntryFactory entryFactory) throws IOException {
      super.writeTo(output, entryFactory);
      output.writeObject(getOrigin());
      output.writeBoolean(parallelStream);
      output.writeObject(segments);
      MarshallUtil.marshallCollection(keys, output);
      MarshallUtil.marshallCollection(excludedKeys, output);
      output.writeBoolean(includeLoader);
      output.writeBoolean(entryStream);
      output.writeObject(intOps);

   }

   @Override
   public void readFrom(UserObjectInput input) throws IOException, ClassNotFoundException {
      super.readFrom(input);
      setOrigin((Address) input.readObject());
      parallelStream = input.readBoolean();
      segments = (IntSet) input.readObject();
      keys = MarshallUtil.unmarshallCollectionUnbounded(input, HashSet::new);
      excludedKeys = MarshallUtil.unmarshallCollectionUnbounded(input, HashSet::new);
      includeLoader = input.readBoolean();
      entryStream = input.readBoolean();
      intOps = (Iterable<IntermediateOperation>) input.readObject();
   }

   @Override
   public boolean isReturnValueExpected() {
      return true;
   }

   @Override
   public boolean canBlock() {
      return true;
   }
}
