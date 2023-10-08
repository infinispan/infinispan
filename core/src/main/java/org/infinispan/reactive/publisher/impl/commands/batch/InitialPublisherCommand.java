package org.infinispan.reactive.publisher.impl.commands.batch;

import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import org.infinispan.commands.TopologyAffectedCommand;
import org.infinispan.commands.functional.functions.InjectableComponent;
import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.commons.util.IntSet;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.marshall.protostream.impl.MarshallableSet;
import org.infinispan.marshall.protostream.impl.WrappedMessages;
import org.infinispan.protostream.WrappedMessage;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.reactive.publisher.impl.DeliveryGuarantee;
import org.infinispan.reactive.publisher.impl.PublisherHandler;
import org.infinispan.util.ByteString;
import org.reactivestreams.Publisher;

@ProtoTypeId(ProtoStreamTypeIds.INITIAL_PUBLISHER_COMMAND)
public class InitialPublisherCommand<K, I, R> extends BaseRpcCommand implements TopologyAffectedCommand {
   public static final byte COMMAND_ID = 18;

   final String requestId;
   final DeliveryGuarantee deliveryGuarantee;
   final int batchSize;
   final IntSet segments;
   final Set<K> keys;
   final Set<K> excludedKeys;
   final long explicitFlags;
   final boolean entryStream;
   final boolean trackKeys;
   final Function<? super Publisher<I>, ? extends Publisher<R>> transformer;
   private int topologyId = -1;

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


   @ProtoFactory
   InitialPublisherCommand(ByteString cacheName, String requestId, DeliveryGuarantee deliveryGuarantee, int batchSize,
                           WrappedMessage wrappedSegments, MarshallableSet<K> wrappedKeys, MarshallableSet<K> wrappedExcludedKeys,
                           long explicitFlags, boolean entryStream, boolean trackKeys, int topologyId,
                           MarshallableObject<Function<? super Publisher<I>, ? extends Publisher<R>>> wrappedTransformer) {
      super(cacheName);
      this.requestId = requestId;
      this.deliveryGuarantee = deliveryGuarantee;
      this.batchSize = batchSize;
      this.segments = WrappedMessages.unwrap(wrappedSegments);
      this.keys = MarshallableSet.unwrap(wrappedKeys);
      this.excludedKeys = MarshallableSet.unwrap(wrappedExcludedKeys);
      this.explicitFlags = explicitFlags;
      this.entryStream = entryStream;
      this.trackKeys = trackKeys;
      this.transformer = MarshallableObject.unwrap(wrappedTransformer);
      this.topologyId = topologyId;
   }

   @ProtoField(2)
   public String getRequestId() {
      return requestId;
   }

   @ProtoField(3)
   public DeliveryGuarantee getDeliveryGuarantee() {
      return deliveryGuarantee;
   }

   @ProtoField(4)
   public int getBatchSize() {
      return batchSize;
   }

   public IntSet getSegments() {
      return segments;
   }

   @ProtoField(5)
   WrappedMessage getWrappedSegments() {
      return WrappedMessages.orElseNull(segments);
   }

   public Set<K> getKeys() {
      return keys;
   }

   @ProtoField(number = 6, name = "keys")
   MarshallableSet<K> getWrappedKeys() {
      return MarshallableSet.create(keys);
   }

   public Set<K> getExcludedKeys() {
      return excludedKeys;
   }

   @ProtoField(number = 7, name = "excludedKeys")
   MarshallableSet<K> getWrappedExcludedKeys() {
      return MarshallableSet.create(excludedKeys);
   }

   @ProtoField(8)
   public long getExplicitFlags() {
      return explicitFlags;
   }

   @ProtoField(9)
   public boolean isEntryStream() {
      return entryStream;
   }

   @ProtoField(10)
   public boolean isTrackKeys() {
      return trackKeys;
   }

   public Function<? super Publisher<I>, ? extends Publisher<R>> getTransformer() {
      return transformer;
   }

   @ProtoField(number = 11, name = "transformer")
   MarshallableObject<Function<? super Publisher<I>, ? extends Publisher<R>>> getWrappedTransformer() {
      return MarshallableObject.create(transformer);
   }

   @Override
   @ProtoField(12)
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
   public CompletionStage<?> invokeAsync(ComponentRegistry componentRegistry) throws Throwable {
      if (transformer instanceof InjectableComponent) {
         ((InjectableComponent) transformer).inject(componentRegistry);
      }

      PublisherHandler publisherHandler = componentRegistry.getPublisherHandler().running();
      return publisherHandler.register(this);
   }
}
