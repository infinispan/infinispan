package org.infinispan.xsite.commands;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.CompletionStage;

import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.util.ByteString;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.xsite.status.TakeOfflineManager;

/**
 * Amend a sites offline status.
 *
 * @author Ryan Emerson
 * @since 11.0
 */
public class XSiteAmendOfflineStatusCommand extends BaseRpcCommand {

   public static final int COMMAND_ID = 103;

   private String siteName;
   private Integer afterFailures;
   private Long minTimeToWait;

   // For CommandIdUniquenessTest only
   public XSiteAmendOfflineStatusCommand() {
      this(null);
   }

   public XSiteAmendOfflineStatusCommand(ByteString cacheName) {
      this(cacheName, null, null, null);
   }

   public XSiteAmendOfflineStatusCommand(ByteString cacheName, String siteName, Integer afterFailures, Long minTimeToWait) {
      super(cacheName);
      this.siteName = siteName;
      this.afterFailures = afterFailures;
      this.minTimeToWait = minTimeToWait;
   }

   @Override
   public CompletionStage<?> invokeAsync(ComponentRegistry registry) throws Throwable {
      TakeOfflineManager takeOfflineManager = registry.getTakeOfflineManager().running();
      takeOfflineManager.amendConfiguration(siteName, afterFailures, minTimeToWait);
      return CompletableFutures.completedNull();
   }

   @Override
   public final boolean isReturnValueExpected() {
      return false;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      output.writeUTF(siteName);
      output.writeObject(afterFailures);
      output.writeObject(minTimeToWait);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      siteName = input.readUTF();
      afterFailures = (Integer) input.readObject();
      minTimeToWait = (Long) input.readObject();
   }

   @Override
   public String toString() {
      return "XSiteAmendOfflineStatusCommand{" +
            "siteName='" + siteName + '\'' +
            ", afterFailures=" + afterFailures +
            ", minTimeToWait=" + minTimeToWait +
            '}';
   }
}
