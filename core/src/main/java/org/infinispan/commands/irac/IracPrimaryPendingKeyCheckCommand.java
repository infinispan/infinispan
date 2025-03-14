package org.infinispan.commands.irac;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CompletionStage;

import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.ByteString;
import org.infinispan.xsite.irac.IracManagerKeyInfo;

/**
 * A command received by the primary owner with a list of possible stale keys.
 *
 * @since 15.2
 */
public class IracPrimaryPendingKeyCheckCommand implements CacheRpcCommand {

   public static final byte COMMAND_ID = 15;

   private Address origin;
   private ByteString cacheName;
   private Collection<IracManagerKeyInfo> keys;

   @SuppressWarnings("unused")
   public IracPrimaryPendingKeyCheckCommand() {
   }

   public IracPrimaryPendingKeyCheckCommand(ByteString cacheName) {
      this.cacheName = cacheName;
   }

   public IracPrimaryPendingKeyCheckCommand(ByteString cacheName, Collection<IracManagerKeyInfo> keys) {
      this.cacheName = cacheName;
      this.keys = keys;
   }

   @Override
   public CompletionStage<Void> invokeAsync(ComponentRegistry registry) {
      registry.getIracManager().running().checkStaleKeys(origin, keys);
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
      MarshallUtil.marshallCollection(keys, output, IracManagerKeyInfo::writeTo);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      keys = MarshallUtil.unmarshallCollection(input, ArrayList::new, IracManagerKeyInfo::readFrom);
   }

   @Override
   public ByteString getCacheName() {
      return cacheName;
   }

   @Override
   public Address getOrigin() {
      return origin;
   }

   @Override
   public void setOrigin(Address origin) {
      this.origin = origin;
   }

   @Override
   public String toString() {
      return "IracTombstonePrimaryCheckCommand{" +
            "cacheName=" + cacheName +
            ", keys=" + keys +
            '}';
   }
}
