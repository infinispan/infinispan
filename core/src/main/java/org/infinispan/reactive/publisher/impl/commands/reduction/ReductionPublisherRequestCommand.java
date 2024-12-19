package org.infinispan.reactive.publisher.impl.commands.reduction;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import org.infinispan.commands.TopologyAffectedCommand;
import org.infinispan.commands.functional.functions.InjectableComponent;
import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.commons.util.EnumUtil;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.Util;
import org.infinispan.context.Flag;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.marshall.protostream.impl.MarshallableCollection;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.marshall.protostream.impl.WrappedMessages;
import org.infinispan.protostream.WrappedMessage;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.reactive.publisher.impl.DeliveryGuarantee;
import org.infinispan.reactive.publisher.impl.LocalPublisherManager;
import org.infinispan.remoting.transport.jgroups.JGroupsAddress;
import org.infinispan.util.ByteString;

/**
 * Stream request command that is sent to remote nodes handle execution of remote intermediate and terminal operations.
 * @param <K> the key type
 */
@ProtoTypeId(ProtoStreamTypeIds.REDUCTION_PUBLISHER_REQUEST_COMMAND)
public class ReductionPublisherRequestCommand<K> extends BaseRpcCommand implements TopologyAffectedCommand {
   public static final byte COMMAND_ID = 31;

   @ProtoField(number = 2, defaultValue = "false")
   final boolean parallelStream;

   @ProtoField(3)
   final DeliveryGuarantee deliveryGuarantee;

   @ProtoField(number = 4, defaultValue = "0")
   final long explicitFlags;

   @ProtoField(number = 5, defaultValue = "false")
   final boolean entryStream;

   final IntSet segments;
   final Set<K> keys;
   final Set<K> excludedKeys;
   final Function<?, ?> transformer;
   final Function<?, ?> finalizer;

   private int topologyId = -1;

   public ReductionPublisherRequestCommand(ByteString cacheName, boolean parallelStream, DeliveryGuarantee deliveryGuarantee,
                                           IntSet segments, Set<K> keys, Set<K> excludedKeys, long explicitFlags, boolean entryStream,
                                           Function<?, ?> transformer, Function<?, ?> finalizer) {
      super(cacheName);
      this.parallelStream = parallelStream;
      this.deliveryGuarantee = deliveryGuarantee;
      this.segments = segments;
      this.keys = keys;
      this.excludedKeys = excludedKeys;
      this.explicitFlags = explicitFlags;
      this.entryStream = entryStream;
      this.transformer = transformer;
      this.finalizer = finalizer;
   }

   @ProtoFactory
   ReductionPublisherRequestCommand(ByteString cacheName, boolean parallelStream, DeliveryGuarantee deliveryGuarantee,
                                    WrappedMessage wrappedSegments, MarshallableCollection<K> keys, long explicitFlags, boolean entryStream,
                                    MarshallableCollection<K> excludedKeys, MarshallableObject<Function<?, ?>> transformer,
                                    MarshallableObject<Function<?, ?>> finalizer, JGroupsAddress origin) {
      super(cacheName);
      this.parallelStream = parallelStream;
      this.deliveryGuarantee = deliveryGuarantee;
      this.segments = WrappedMessages.unwrap(wrappedSegments);
      this.keys = MarshallableCollection.unwrap(keys, HashSet::new);
      this.excludedKeys = MarshallableCollection.unwrap(excludedKeys, HashSet::new);
      this.explicitFlags = explicitFlags;
      this.entryStream = entryStream;
      this.finalizer = MarshallableObject.unwrap(finalizer);
      this.transformer = transformer == null ? this.finalizer : MarshallableObject.unwrap(transformer);
      this.origin = origin;
   }

   @ProtoField(6)
   WrappedMessage getWrappedSegments() {
      return WrappedMessages.orElseNull(segments);
   }

   @ProtoField(7)
   MarshallableCollection<K> getKeys() {
      return MarshallableCollection.create(keys);
   }

   @ProtoField(8)
   MarshallableCollection<K> getExcludedKeys() {
      return MarshallableCollection.create(excludedKeys);
   }

   @ProtoField(9)
   MarshallableObject<Function<?, ?>> getTransformer() {
      // If transformer is the same as the finalizer, then only set the finalizer field
      return transformer == finalizer ? null : MarshallableObject.create(transformer);
   }

   @ProtoField(10)
   MarshallableObject<Function<?, ?>> getFinalizer() {
      return MarshallableObject.create(finalizer);
   }

   @ProtoField(11)
   public JGroupsAddress getOrigin() {
      return (JGroupsAddress) origin;
   }

   @Override
   public CompletionStage<?> invokeAsync(ComponentRegistry componentRegistry) throws Throwable {
      if (transformer instanceof InjectableComponent) {
         ((InjectableComponent) transformer).inject(componentRegistry);
      }
      if (finalizer instanceof InjectableComponent) {
         ((InjectableComponent) finalizer).inject(componentRegistry);
      }
      LocalPublisherManager lpm = componentRegistry.getLocalPublisherManager().running();
      if (entryStream) {
         return lpm.entryReduction(parallelStream, segments, keys, excludedKeys,
               explicitFlags, deliveryGuarantee, transformer, finalizer);
      } else {
         return lpm.keyReduction(parallelStream, segments, keys, excludedKeys,
               explicitFlags, deliveryGuarantee, transformer, finalizer);
      }
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
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
   public boolean isReturnValueExpected() {
      return true;
   }

   @Override
   public String toString() {
      return "PublisherRequestCommand{" +
             ", flags=" + EnumUtil.prettyPrintBitSet(explicitFlags, Flag.class) +
             ", topologyId=" + topologyId +
             ", segments=" + segments +
             ", keys=" + Util.toStr(keys) +
             ", excludedKeys=" + Util.toStr(excludedKeys) +
             ", transformer= " + transformer +
             ", finalizer=" + finalizer +
             '}';
   }
}
