package org.infinispan.commands.irac;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.CompletionStage;

import org.infinispan.commons.util.Util;
import org.infinispan.distribution.DistributionInfo;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.metadata.impl.IracMetadata;
import org.infinispan.remoting.responses.ValidResponse;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.ValidSingleResponseCollector;
import org.infinispan.util.ByteString;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.xsite.BackupReceiver;
import org.infinispan.xsite.XSiteReplicateCommand;
import org.infinispan.xsite.irac.IracManager;

/**
 * A {@link XSiteReplicateCommand} to check and cleanup tombstones for IRAC algorithm.
 * <p>
 * This command has 2 modes: (1, when tombstone==null) when it is sent to a remote site, it checks if the key exists in
 * the {@link IracManager}. This check is performed in the primary owner of the key; (2, when tombstone!=null) if it is
 * sent from primary owner to backup owners, the backup owners remove the tombstone.
 *
 * @since 14.0
 */
public class IracCleanupTombstoneCommand extends XSiteReplicateCommand<Boolean> {

   public static final byte COMMAND_ID = 37;

   private Object key;
   private IracMetadata tombstone;

   @SuppressWarnings("unused")
   public IracCleanupTombstoneCommand() {
      super(COMMAND_ID, null);
   }

   public IracCleanupTombstoneCommand(ByteString cacheName) {
      super(COMMAND_ID, cacheName);
   }

   public IracCleanupTombstoneCommand(ByteString cacheName, Object key, IracMetadata tombstone) {
      super(COMMAND_ID, cacheName);
      this.key = key;
      this.tombstone = tombstone;
   }

   @Override
   public ByteString getCacheName() {
      return cacheName;
   }

   @Override
   public CompletionStage<Boolean> invokeAsync(ComponentRegistry registry) {
      if (tombstone == null) {
         // command received from a remote site.
         // check if the key exists in IracManager
         return isKeyInIracManager(registry);
      }
      // removes the tombstone
      registry.getIracTombstoneCleaner().running().removeTombstone(key, tombstone);
      return CompletableFutures.completedNull();
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
      IracMetadata.writeTo(output, tombstone);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      this.key = input.readObject();
      this.tombstone = IracMetadata.readFrom(input);
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
      return "IracCleanupTombstoneCommand{" +
            "cacheName=" + cacheName +
            ", key=" + Util.toStr(key) +
            ", tombstone=" + tombstone +
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
