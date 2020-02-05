package org.infinispan.commands.topology;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.CompletionStage;

import org.infinispan.factories.GlobalComponentRegistry;

/**
 * The coordinator is requesting information about the running caches.
 *
 * @author Ryan Emerson
 * @since 11.0
 */
public class CacheStatusRequestCommand extends AbstractCacheControlCommand {

   public static final byte COMMAND_ID = 96;

   private int viewId;

   // For CommandIdUniquenessTest only
   public CacheStatusRequestCommand() {
      super(COMMAND_ID);
   }

   public CacheStatusRequestCommand(int viewId) {
      super(COMMAND_ID);
      this.viewId = viewId;
   }

   @Override
   public CompletionStage<?> invokeAsync(GlobalComponentRegistry gcr) throws Throwable {
      return gcr.getLocalTopologyManager()
            .handleStatusRequest(viewId);
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      output.writeInt(viewId);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      viewId = input.readInt();
   }

   @Override
   public String toString() {
      return "CacheStatusCommand{" +
            "viewId=" + viewId +
            '}';
   }
}
