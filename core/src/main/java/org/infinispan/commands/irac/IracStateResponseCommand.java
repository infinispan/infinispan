package org.infinispan.commands.irac;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.CompletionStage;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.commons.util.Util;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.metadata.impl.IracMetadata;
import org.infinispan.protostream.annotations.Proto;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.util.ByteString;
import org.infinispan.xsite.irac.IracManager;
import org.infinispan.xsite.irac.IracManagerKeyInfo;

/**
 * The IRAC state for a given key.
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
@ProtoTypeId(ProtoStreamTypeIds.IRAC_STATE_RESPONSE_COMMAND)
public class IracStateResponseCommand extends BaseIracCommand {

   @ProtoField(2)
   final Collection<State> stateCollection;

   @ProtoFactory
   IracStateResponseCommand(ByteString cacheName, ArrayList<State> stateCollection) {
      super(cacheName);
      this.stateCollection = stateCollection;
   }

   public IracStateResponseCommand(ByteString cacheName, int capacity) {
      super(cacheName);
      this.stateCollection = new ArrayList<>(capacity);
   }

   @Override
   public CompletionStage<?> invokeAsync(ComponentRegistry registry) {
      IracManager manager = registry.getIracManager().running();
      for (State state : stateCollection) {
         state.apply(manager);
      }
      return CompletableFutures.completedNull();
   }

   public void add(IracManagerKeyInfo keyInfo, IracMetadata tombstone) {
      stateCollection.add(new State(keyInfo, tombstone));
   }

   @Override
   public String toString() {
      return "IracStateResponseCommand{" +
            "cacheName=" + cacheName +
            ", state=" + Util.toStr(stateCollection) +
            '}';
   }

   @Proto
   @ProtoTypeId(ProtoStreamTypeIds.IRAC_STATE_RESPONSE_COMMAND_STATE)
   public record State(IracManagerKeyInfo keyInfo, IracMetadata tombstone) {
      public State {
         Objects.requireNonNull(keyInfo);
      }

      void apply(IracManager manager) {
         manager.receiveState(keyInfo.getSegment(), keyInfo.getKey(), keyInfo.getOwner(), tombstone);
      }
   }
}
