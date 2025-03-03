package org.infinispan.commands.irac;

import java.util.Collection;
import java.util.concurrent.CompletionStage;

import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.container.versioning.irac.IracTombstoneInfo;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.util.ByteString;
import org.infinispan.xsite.irac.IracManager;

/**
 * A {@link CacheRpcCommand} to check if one or more tombstones are still valid.
 * <p>
 * Periodically, the backup owner send this command for the tombstones they have stored if the key is not present in
 * {@link IracManager}.
 *
 * @since 14.0
 */
@ProtoTypeId(ProtoStreamTypeIds.IRAC_TOMBSTONE_PRIMARY_CHECK_COMMAND)
public class IracTombstonePrimaryCheckCommand extends BaseIracCommand {

   @ProtoField(2)
   final Collection<IracTombstoneInfo> tombstoneToCheck;

   @ProtoFactory
   public IracTombstonePrimaryCheckCommand(ByteString cacheName, Collection<IracTombstoneInfo> tombstoneToCheck) {
      super(cacheName);
      this.tombstoneToCheck = tombstoneToCheck;
   }

   @Override
   public CompletionStage<Void> invokeAsync(ComponentRegistry registry) {
      registry.getIracTombstoneManager().running().checkStaleTombstone(tombstoneToCheck);
      return CompletableFutures.completedNull();
   }

   @Override
   public String toString() {
      return "IracTombstonePrimaryCheckCommand{" +
            "cacheName=" + cacheName +
            ", tombstoneToCheck=" + tombstoneToCheck +
            '}';
   }

   public Collection<IracTombstoneInfo> getTombstoneToCheck() {
      return tombstoneToCheck;
   }
}
