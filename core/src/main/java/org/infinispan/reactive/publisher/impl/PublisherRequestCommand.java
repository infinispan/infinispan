package org.infinispan.reactive.publisher.impl;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import org.infinispan.commands.TopologyAffectedCommand;
import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.Util;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.ByteString;

/**
 * Stream request command that is sent to remote nodes handle execution of remote intermediate and terminal operations.
 * @param <K> the key type
 */
public class PublisherRequestCommand<K> extends BaseRpcCommand implements TopologyAffectedCommand {
   public static final byte COMMAND_ID = 31;

   private LocalPublisherManager lpm;

   private boolean parallelStream;
   private DeliveryGuarantee deliveryGuarantee;
   private IntSet segments;
   private Set<K> keys;
   private Set<K> excludedKeys;
   private boolean includeLoader;
   private boolean entryStream;
   private Function transformer;
   private Function finalizer;
   private int topologyId = -1;

   @Override
   public int getTopologyId() {
      return topologyId;
   }

   @Override
   public void setTopologyId(int topologyId) {
      this.topologyId = topologyId;
   }

   // Only here for CommandIdUniquenessTest
   private PublisherRequestCommand() { super(null); }

   public PublisherRequestCommand(ByteString cacheName) {
      super(cacheName);
   }

   public PublisherRequestCommand(ByteString cacheName, boolean parallelStream, DeliveryGuarantee deliveryGuarantee,
         IntSet segments, Set<K> keys, Set<K> excludedKeys, boolean includeLoader, boolean entryStream,
         Function transformer, Function finalizer) {
      super(cacheName);
      this.parallelStream = parallelStream;
      this.deliveryGuarantee = deliveryGuarantee;
      this.segments = segments;
      this.keys = keys;
      this.excludedKeys = excludedKeys;
      this.includeLoader = includeLoader;
      this.entryStream = entryStream;
      this.transformer = transformer;
      this.finalizer = finalizer;
   }

   public void inject(LocalPublisherManager lpm) {
      this.lpm = lpm;
   }

   @Override
   public CompletableFuture<Object> invokeAsync() throws Throwable {
      if (entryStream) {
         return (CompletableFuture<Object>) lpm.entryReduction(parallelStream, segments, keys, excludedKeys,
               includeLoader, deliveryGuarantee, transformer, finalizer);
      } else {
         return (CompletableFuture<Object>) lpm.keyReduction(parallelStream, segments, keys, excludedKeys,
               includeLoader, deliveryGuarantee, transformer, finalizer);
      }
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      output.writeObject(getOrigin());
      output.writeBoolean(parallelStream);
      MarshallUtil.marshallEnum(deliveryGuarantee, output);
      output.writeObject(segments);
      MarshallUtil.marshallCollection(keys, output);
      MarshallUtil.marshallCollection(excludedKeys, output);
      output.writeBoolean(includeLoader);
      output.writeBoolean(entryStream);
      output.writeObject(transformer);
      output.writeObject(finalizer);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      setOrigin((Address) input.readObject());
      parallelStream = input.readBoolean();
      deliveryGuarantee = MarshallUtil.unmarshallEnum(input, DeliveryGuarantee::valueOf);
      segments = (IntSet) input.readObject();
      keys = MarshallUtil.unmarshallCollectionUnbounded(input, HashSet::new);
      excludedKeys = MarshallUtil.unmarshallCollectionUnbounded(input, HashSet::new);
      includeLoader = input.readBoolean();
      entryStream = input.readBoolean();
      transformer = (Function) input.readObject();
      finalizer = (Function) input.readObject();
   }

   @Override
   public boolean isReturnValueExpected() {
      return true;
   }

   @Override
   public boolean canBlock() {
      // When the stream isn't parallel the data container iteration is done by the calling thread - and the user
      // provides the transformation, so it could block - this technically slows down sequential for some cases, but
      // is much safer
      return !parallelStream;
   }

   @Override
   public String toString() {
      return "PublisherRequestCommand{" +
            ", includeLoader=" + includeLoader +
            ", topologyId=" + topologyId +
            ", segments=" + segments +
            ", keys=" + Util.toStr(keys) +
            ", excludedKeys=" + Util.toStr(excludedKeys) +
            ", transformer= " + transformer +
            ", finalizer=" + finalizer +
            '}';
   }
}
