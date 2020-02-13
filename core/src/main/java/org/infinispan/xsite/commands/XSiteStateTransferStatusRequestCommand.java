package org.infinispan.xsite.commands;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.util.ByteString;
import org.infinispan.xsite.statetransfer.XSiteStateTransferManager;

/**
 * Get XSite state transfer status.
 *
 * @author Ryan Emerson
 * @since 11.0
 */
public class XSiteStateTransferStatusRequestCommand extends BaseRpcCommand {

   public static final byte COMMAND_ID = 109;

   // For CommandIdUniquenessTest only
   public XSiteStateTransferStatusRequestCommand() {
      super(null);
   }

   public XSiteStateTransferStatusRequestCommand(ByteString cacheName) {
      super(cacheName);
   }

   @Override
   public CompletionStage<?> invokeAsync(ComponentRegistry registry) {
      XSiteStateTransferManager stateTransferManager = registry.getXSiteStateTransferManager().running();
      return CompletableFuture.completedFuture(stateTransferManager.getStatus());
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public boolean isReturnValueExpected() {
      return true;
   }

   @Override
   public String toString() {
      return "XSiteStateTransferStatusRequestCommand{" +
            "cacheName=" + cacheName +
            '}';
   }
}
