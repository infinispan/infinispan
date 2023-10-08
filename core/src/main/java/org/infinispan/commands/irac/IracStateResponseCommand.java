package org.infinispan.commands.irac;

import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CompletionStage;

import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.commons.util.Util;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.metadata.impl.IracMetadata;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.remoting.transport.Address;
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
public class IracStateResponseCommand extends BaseRpcCommand {

   public static final byte COMMAND_ID = 120;

   @ProtoField(number = 2, collectionImplementation = ArrayList.class)
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

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public boolean isReturnValueExpected() {
      return false;
   }

   @Override
   public Address getOrigin() {
      //no-op
      return null;
   }

   @Override
   public void setOrigin(Address origin) {
      //no-op
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

   @ProtoTypeId(ProtoStreamTypeIds.IRAC_STATE_RESPONSE_COMMAND_STATE)
   public static class State {
      @ProtoField(1)
      final IracManagerKeyInfo keyInfo;
      @ProtoField(2)
      final IracMetadata tombstone;

      @ProtoFactory
      State(IracManagerKeyInfo keyInfo, IracMetadata tombstone) {
         this.keyInfo = requireNonNull(keyInfo);
         this.tombstone = tombstone;
      }

      void apply(IracManager manager) {
         manager.receiveState(keyInfo.getSegment(), keyInfo.getKey(), keyInfo.getOwner(), tombstone);
      }

      @Override
      public String toString() {
         return "State{" +
               "keyInfo=" + keyInfo +
               ", tombstone=" + tombstone +
               '}';
      }
   }
}
