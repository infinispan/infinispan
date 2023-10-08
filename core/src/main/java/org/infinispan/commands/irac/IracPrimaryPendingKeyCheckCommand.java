package org.infinispan.commands.irac;

import java.util.Collection;
import java.util.concurrent.CompletionStage;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.ByteString;
import org.infinispan.xsite.irac.IracManagerKeyInfo;

/**
 * A command received by the primary owner with a list of possible stale keys.
 *
 * @since 15.2
 */
@ProtoTypeId(ProtoStreamTypeIds.IRAC_PRIMARY_PENDING_KEY_CHECK)
public class IracPrimaryPendingKeyCheckCommand extends BaseIracCommand {

   public static final byte COMMAND_ID = 15;

   private Address origin;

   @ProtoField(2)
   final Collection<IracManagerKeyInfo> keys;

   @ProtoFactory
   public IracPrimaryPendingKeyCheckCommand(ByteString cacheName, Collection<IracManagerKeyInfo> keys) {
      super(cacheName);
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
