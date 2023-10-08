package org.infinispan.commands.irac;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.concurrent.CompletionStage;

import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.container.versioning.irac.IracTombstoneInfo;
import org.infinispan.container.versioning.irac.IracTombstoneManager;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.util.ByteString;

/**
 * A {@link CacheRpcCommand} to clean up tombstones for IRAC algorithm.
 * <p>
 * This command is sent from the primary owners to the backup owner with the tombstone to be removed. No response is
 * expected from the backup owners.
 *
 * @since 14.0
 */
@ProtoTypeId(ProtoStreamTypeIds.IRAC_TOMBSTONE_CLEANUP_COMMAND)
public class IracTombstoneCleanupCommand extends BaseIracCommand {

   public static final byte COMMAND_ID = 37;

   @ProtoField(2)
   final Collection<IracTombstoneInfo> tombstonesToRemove;


   @ProtoFactory
   IracTombstoneCleanupCommand(ByteString cacheName, ArrayList<IracTombstoneInfo> tombstonesToRemove) {
      super(cacheName);
      this.tombstonesToRemove = tombstonesToRemove;
   }

   public IracTombstoneCleanupCommand(ByteString cacheName, int maxCapacity) {
      super(cacheName);
      tombstonesToRemove = new HashSet<>(maxCapacity);
   }

   @Override
   public CompletionStage<Boolean> invokeAsync(ComponentRegistry registry) {
      IracTombstoneManager tombstoneManager = registry.getIracTombstoneManager().running();
      tombstonesToRemove.forEach(tombstoneManager::removeTombstone);
      return CompletableFutures.completedNull();
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public String toString() {
      return "IracTombstoneCleanupCommand{" +
            "cacheName=" + cacheName +
            ", tombstone=" + tombstonesToRemove +
            '}';
   }

   public void add(IracTombstoneInfo tombstone) {
      tombstonesToRemove.add(tombstone);
   }

   public int size() {
      return tombstonesToRemove.size();
   }

   public boolean isEmpty() {
      return tombstonesToRemove.isEmpty();
   }

   public Collection<IracTombstoneInfo> getTombstonesToRemove() {
      return tombstonesToRemove;
   }
}
