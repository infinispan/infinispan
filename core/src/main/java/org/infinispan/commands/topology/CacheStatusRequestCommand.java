package org.infinispan.commands.topology;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.topology.ManagerStatusResponse;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * The coordinator is requesting information about the running caches.
 *
 * @author Ryan Emerson
 * @since 11.0
 */
public class CacheStatusRequestCommand extends AbstractCacheControlCommand {

   private static final Log log = LogFactory.getLog(CacheStatusRequestCommand.class);

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
      if (!gcr.isLocalTopologyManagerRunning()) {
         log.debug("Reply with empty status request because topology manager not running");
         return CompletableFuture.completedFuture(new ManagerStatusResponse(Collections.emptyMap(), true));
      }

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
      return "CacheStatusRequestCommand{" +
            "viewId=" + viewId +
            '}';
   }
}
