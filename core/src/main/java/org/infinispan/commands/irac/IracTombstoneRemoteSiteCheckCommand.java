package org.infinispan.commands.irac;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.CompletionStage;

import org.infinispan.commons.util.Util;
import org.infinispan.distribution.DistributionInfo;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.remoting.responses.ValidResponse;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.ValidSingleResponseCollector;
import org.infinispan.util.ByteString;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.xsite.BackupReceiver;
import org.infinispan.xsite.XSiteReplicateCommand;

/**
 * A {@link XSiteReplicateCommand} to check tombstones for IRAC algorithm.
 * <p>
 * Periodically, the primary owner sends this command to the remote sites where they check if the tombstone for this key
 * is still necessary.
 *
 * @since 14.0
 */
public class IracTombstoneRemoteSiteCheckCommand extends XSiteReplicateCommand<Boolean> {

   public static final byte COMMAND_ID = 38;

   // TODO add batching https://issues.redhat.com/browse/ISPN-13496
   private Object key;

   @SuppressWarnings("unused")
   public IracTombstoneRemoteSiteCheckCommand() {
      super(COMMAND_ID, null);
   }

   public IracTombstoneRemoteSiteCheckCommand(ByteString cacheName) {
      super(COMMAND_ID, cacheName);
   }

   public IracTombstoneRemoteSiteCheckCommand(ByteString cacheName, Object key) {
      super(COMMAND_ID, cacheName);
      this.key = key;
   }

   @Override
   public ByteString getCacheName() {
      return cacheName;
   }

   @Override
   public CompletionStage<Boolean> invokeAsync(ComponentRegistry registry) {
      return isKeyInIracManager(registry);
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public CompletionStage<Boolean> performInLocalSite(ComponentRegistry registry, boolean preserveOrder) {
      DistributionInfo distribution = registry.getDistributionManager().getCacheTopology().getDistribution(key);
      if (distribution.isPrimary()) {
         return isKeyInIracManager(registry);
      } else {
         RpcManager manager = registry.getRpcManager().running();
         return manager.invokeCommand(distribution.primary(), this, new BooleanResponseCollector(), manager.getSyncRpcOptions());
      }
   }

   @Override
   public CompletionStage<Boolean> performInLocalSite(BackupReceiver receiver, boolean preserveOrder) {
      throw new IllegalStateException("Should never be invoked!");
   }

   @Override
   public boolean isReturnValueExpected() {
      return true;
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      output.writeObject(key);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      this.key = input.readObject();
   }

   @Override
   public Address getOrigin() {
      //not needed
      return null;
   }

   @Override
   public void setOrigin(Address origin) {
      //no-op
   }

   @Override
   public String toString() {
      return "IracSiteTombstoneCheckCommand{" +
            "cacheName=" + cacheName +
            ", key=" + Util.toStr(key) +
            '}';
   }

   private CompletionStage<Boolean> isKeyInIracManager(ComponentRegistry registry) {
      return CompletableFutures.booleanStage(registry.getIracManager().running().containsKey(key));
   }

   private static final class BooleanResponseCollector extends ValidSingleResponseCollector<Boolean> {

      @Override
      protected Boolean withValidResponse(Address sender, ValidResponse response) {
         return (Boolean) response.getResponseValue();
      }

      @Override
      protected Boolean targetNotFound(Address sender) {
         return Boolean.TRUE;
      }
   }
}
