package org.infinispan.xsite.commands;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.util.ByteString;
import org.infinispan.xsite.status.TakeOfflineManager;

/**
 * Take a site offline.
 *
 * @author Ryan Emerson
 * @since 11.0
 */
public class XSiteBringOnlineCommand extends BaseRpcCommand {

   public static final int COMMAND_ID = 102;

   private String siteName;

   // For CommandIdUniquenessTest only
   public XSiteBringOnlineCommand() {
      this(null);
   }

   public XSiteBringOnlineCommand(ByteString cacheName) {
      this(cacheName, null);
   }

   public XSiteBringOnlineCommand(ByteString cacheName, String siteName) {
      super(cacheName);
      this.siteName = siteName;
   }

   @Override
   public CompletionStage<?> invokeAsync(ComponentRegistry registry) throws Throwable {
      TakeOfflineManager takeOfflineManager = registry.getTakeOfflineManager().running();
      return CompletableFuture.completedFuture(takeOfflineManager.bringSiteOnline(siteName));
   }

   @Override
   public final boolean isReturnValueExpected() {
      return true;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      output.writeUTF(siteName);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      siteName = input.readUTF();
   }

   @Override
   public String toString() {
      return "XSiteBringOnlineCommand{" +
            "siteName='" + siteName + '\'' +
            '}';
   }
}
