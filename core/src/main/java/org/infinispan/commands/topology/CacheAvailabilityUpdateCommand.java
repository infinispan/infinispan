package org.infinispan.commands.topology;

import java.util.concurrent.CompletionStage;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.partitionhandling.AvailabilityMode;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * Change the availability of a cache.
 *
 * @author Ryan Emerson
 * @since 11.0
 */
@ProtoTypeId(ProtoStreamTypeIds.CACHE_AVAILABILITY_UPDATE_COMMAND)
public class CacheAvailabilityUpdateCommand extends AbstractCacheControlCommand {

   public static final byte COMMAND_ID = 98;

   @ProtoField(1)
   final String cacheName;

   @ProtoField(2)
   final AvailabilityMode availabilityMode;

   @ProtoFactory
   public CacheAvailabilityUpdateCommand(String cacheName, AvailabilityMode availabilityMode) {
      super(COMMAND_ID);
      this.cacheName = cacheName;
      this.availabilityMode = availabilityMode;
   }

   @Override
   public CompletionStage<?> invokeAsync(GlobalComponentRegistry gcr) throws Throwable {
      return gcr.getClusterTopologyManager()
            .forceAvailabilityMode(cacheName, availabilityMode);
   }

   @Override
   public String toString() {
      return "UpdateAvailabilityCommand{" +
            "cacheName='" + cacheName + '\'' +
            ", availabilityMode=" + availabilityMode +
            '}';
   }
}
