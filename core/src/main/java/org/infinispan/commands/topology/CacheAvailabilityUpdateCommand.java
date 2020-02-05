package org.infinispan.commands.topology;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.CompletionStage;

import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.partitionhandling.AvailabilityMode;

/**
 * Change the availability of a cache.
 *
 * @author Ryan Emerson
 * @since 11.0
 */
public class CacheAvailabilityUpdateCommand extends AbstractCacheControlCommand {

   public static final byte COMMAND_ID = 98;

   private String cacheName;
   private AvailabilityMode availabilityMode;

   // For CommandIdUniquenessTest only
   public CacheAvailabilityUpdateCommand() {
      super(COMMAND_ID);
   }

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
   public void writeTo(ObjectOutput output) throws IOException {
      MarshallUtil.marshallString(cacheName, output);
      MarshallUtil.marshallEnum(availabilityMode, output);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      cacheName = MarshallUtil.unmarshallString(input);
      availabilityMode = MarshallUtil.unmarshallEnum(input, AvailabilityMode::valueOf);
   }

   @Override
   public String toString() {
      return "UpdateAvailabilityCommand{" +
            "cacheName='" + cacheName + '\'' +
            ", availabilityMode=" + availabilityMode +
            '}';
   }
}
