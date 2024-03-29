package org.infinispan.reactive.publisher.impl.commands.batch;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import org.infinispan.commands.TopologyAffectedCommand;
import org.infinispan.commands.functional.functions.InjectableComponent;
import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.commons.util.IntSet;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.reactive.publisher.impl.DeliveryGuarantee;
import org.infinispan.reactive.publisher.impl.PublisherHandler;
import org.infinispan.util.ByteString;
import org.reactivestreams.Publisher;

public class InitialPublisherCommand<K, I, R> extends BaseRpcCommand implements TopologyAffectedCommand {
   public static final byte COMMAND_ID = 18;

   private String requestId;
   private DeliveryGuarantee deliveryGuarantee;
   private int batchSize;
   private IntSet segments;
   private Set<K> keys;
   private Set<K> excludedKeys;
   private long explicitFlags;
   private boolean entryStream;
   private boolean trackKeys;
   private Function<? super Publisher<I>, ? extends Publisher<R>> transformer;
   private int topologyId = -1;

   // Only here for CommandIdUniquenessTest
   private InitialPublisherCommand() { super(null); }

   public InitialPublisherCommand(ByteString cacheName) {
      super(cacheName);
   }

   public InitialPublisherCommand(ByteString cacheName, String requestId, DeliveryGuarantee deliveryGuarantee,
         int batchSize, IntSet segments, Set<K> keys, Set<K> excludedKeys, long explicitFlags, boolean entryStream,
         boolean trackKeys, Function<? super Publisher<I>, ? extends Publisher<R>> transformer) {
      super(cacheName);
      this.requestId = requestId;
      this.deliveryGuarantee = deliveryGuarantee;
      this.batchSize = batchSize;
      this.segments = segments;
      this.keys = keys;
      this.excludedKeys = excludedKeys;
      this.explicitFlags = explicitFlags;
      this.entryStream = entryStream;
      this.trackKeys = trackKeys;
      this.transformer = transformer;
   }

   public String getRequestId() {
      return requestId;
   }

   public DeliveryGuarantee getDeliveryGuarantee() {
      return deliveryGuarantee;
   }

   public int getBatchSize() {
      return batchSize;
   }

   public IntSet getSegments() {
      return segments;
   }

   public Set<K> getKeys() {
      return keys;
   }

   public Set<K> getExcludedKeys() {
      return excludedKeys;
   }

   public long getExplicitFlags() {
      return explicitFlags;
   }

   public boolean isEntryStream() {
      return entryStream;
   }

   public boolean isTrackKeys() {
      return trackKeys;
   }

   public Function<? super Publisher<I>, ? extends Publisher<R>> getTransformer() {
      return transformer;
   }

   @Override
   public CompletionStage<?> invokeAsync(ComponentRegistry componentRegistry) throws Throwable {
      if (transformer instanceof InjectableComponent) {
         ((InjectableComponent) transformer).inject(componentRegistry);
      }

      PublisherHandler publisherHandler = componentRegistry.getPublisherHandler().running();
      return publisherHandler.register(this);
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
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public boolean isReturnValueExpected() {
      return true;
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      output.writeUTF(requestId);
      MarshallUtil.marshallEnum(deliveryGuarantee, output);
      UnsignedNumeric.writeUnsignedInt(output, batchSize);
      output.writeObject(segments);
      MarshallUtil.marshallCollection(keys, output);
      MarshallUtil.marshallCollection(excludedKeys, output);
      // Maybe put the booleans into a single byte - only saves 2 bytes though
      output.writeLong(explicitFlags);
      output.writeBoolean(entryStream);
      output.writeBoolean(trackKeys);
      output.writeObject(transformer);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      requestId = input.readUTF();
      deliveryGuarantee = MarshallUtil.unmarshallEnum(input, DeliveryGuarantee::valueOf);
      batchSize = UnsignedNumeric.readUnsignedInt(input);
      segments = (IntSet) input.readObject();
      keys = MarshallUtil.unmarshallCollectionUnbounded(input, HashSet::new);
      excludedKeys = MarshallUtil.unmarshallCollectionUnbounded(input, HashSet::new);
      explicitFlags = input.readLong();
      entryStream = input.readBoolean();
      trackKeys = input.readBoolean();
      transformer = (Function) input.readObject();
   }
}
