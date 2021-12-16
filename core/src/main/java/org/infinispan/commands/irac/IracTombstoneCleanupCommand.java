package org.infinispan.commands.irac;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.CompletionStage;

import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.container.versioning.irac.IracTombstoneInfo;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.ByteString;
import org.infinispan.util.concurrent.CompletableFutures;

/**
 * A {@link CacheRpcCommand} to clean up tombstones for IRAC algorithm.
 * <p>
 * This command is sent from the primary owners to the backup owner with the tombstone to be removed. No response is
 * expected from the backup owners.
 *
 * @since 14.0
 */
public class IracTombstoneCleanupCommand implements CacheRpcCommand {

   public static final byte COMMAND_ID = 37;

   private final ByteString cacheName;
   // TODO add batching https://issues.redhat.com/browse/ISPN-13496
   private IracTombstoneInfo tombstone;

   @SuppressWarnings("unused")
   public IracTombstoneCleanupCommand() {
      this(null);
   }

   public IracTombstoneCleanupCommand(ByteString cacheName) {
      this.cacheName = cacheName;
   }

   public IracTombstoneCleanupCommand(ByteString cacheName, IracTombstoneInfo tombstone) {
      this(cacheName);
      this.tombstone = tombstone;
   }

   @Override
   public ByteString getCacheName() {
      return cacheName;
   }

   @Override
   public CompletionStage<Boolean> invokeAsync(ComponentRegistry registry) {
      registry.getIracTombstoneManager().running().removeTombstone(tombstone);
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
      IracTombstoneInfo.writeTo(output, tombstone);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      this.tombstone = IracTombstoneInfo.readFrom(input);
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
      return "IracTombstoneCleanupCommand{" +
            "cacheName=" + cacheName +
            ", tombstone=" + tombstone +
            '}';
   }

   public IracTombstoneInfo getTombstone() {
      return tombstone;
   }
}
