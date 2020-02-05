package org.infinispan.commands.topology;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.CompletionStage;

import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.factories.GlobalComponentRegistry;

/**
 * A member is requesting a cache shutdown.
 *
 * @author Ryan Emerson
 * @since 11.0
 */
public class CacheShutdownRequestCommand extends AbstractCacheControlCommand {

   public static final byte COMMAND_ID = 93;

   private String cacheName;

   // For CommandIdUniquenessTest only
   public CacheShutdownRequestCommand() {
      super(COMMAND_ID);
   }

   public CacheShutdownRequestCommand(String cacheName) {
      super(COMMAND_ID);
      this.cacheName = cacheName;
   }

   @Override
   public CompletionStage<?> invokeAsync(GlobalComponentRegistry gcr) throws Throwable {
      return gcr.getClusterTopologyManager()
            .handleShutdownRequest(cacheName);
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      MarshallUtil.marshallString(cacheName, output);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      cacheName = MarshallUtil.unmarshallString(input);
   }

   @Override
   public String toString() {
      return "ShutdownCacheRequestCommand{" +
            "cacheName='" + cacheName + '\'' +
            '}';
   }
}
