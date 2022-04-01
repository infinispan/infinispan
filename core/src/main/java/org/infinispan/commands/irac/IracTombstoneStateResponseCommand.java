package org.infinispan.commands.irac;

import static org.infinispan.commons.marshall.MarshallUtil.marshallCollection;
import static org.infinispan.commons.marshall.MarshallUtil.unmarshallCollection;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CompletionStage;

import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.container.versioning.irac.IracTombstoneInfo;
import org.infinispan.container.versioning.irac.IracTombstoneManager;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.ByteString;
import org.infinispan.commons.util.concurrent.CompletableFutures;

/**
 * Response for a state request with the tombstones stored in the local node.
 *
 * @since 14.0
 */
public class IracTombstoneStateResponseCommand implements CacheRpcCommand {

   public static final byte COMMAND_ID = 39;

   private ByteString cacheName;
   private Collection<IracTombstoneInfo> state;

   @SuppressWarnings("unused")
   public IracTombstoneStateResponseCommand() {
   }

   public IracTombstoneStateResponseCommand(ByteString cacheName) {
      this.cacheName = cacheName;
   }

   public IracTombstoneStateResponseCommand(ByteString cacheName, Collection<IracTombstoneInfo> state) {
      this.cacheName = cacheName;
      this.state = state;
   }

   @Override
   public CompletionStage<Void> invokeAsync(ComponentRegistry registry) {
      IracTombstoneManager tombstoneManager = registry.getIracTombstoneManager().running();
      for (IracTombstoneInfo data : state) {
         tombstoneManager.storeTombstoneIfAbsent(data);
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
   public void writeTo(ObjectOutput output) throws IOException {
      marshallCollection(state, output, IracTombstoneInfo::writeTo);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      state = unmarshallCollection(input, ArrayList::new, IracTombstoneInfo::readFrom);
   }

   @Override
   public ByteString getCacheName() {
      return cacheName;
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

   @Override
   public String toString() {
      return "IracTombstoneStateResponseCommand{" +
            "cacheName=" + cacheName +
            ", state=" + state +
            '}';
   }
}
