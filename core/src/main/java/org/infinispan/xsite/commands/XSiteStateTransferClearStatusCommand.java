package org.infinispan.xsite.commands;

import java.util.concurrent.CompletionStage;

import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.util.ByteString;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.xsite.statetransfer.XSiteStateTransferManager;

/**
 * Clear XSite state transfer status.
 *
 * @author Ryan Emerson
 * @since 11.0
 */
public class XSiteStateTransferClearStatusCommand extends BaseRpcCommand {

   public static final byte COMMAND_ID = 111;

   // For CommandIdUniquenessTest only
   public XSiteStateTransferClearStatusCommand() {
      super(null);
   }

   public XSiteStateTransferClearStatusCommand(ByteString cacheName) {
      super(cacheName);
   }

   @Override
   public CompletionStage<?> invokeAsync(ComponentRegistry registry) {
      invokeLocal(registry.getXSiteStateTransferManager().running());
      return CompletableFutures.completedNull();
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public boolean isReturnValueExpected() {
      return false;
   }

   @Override
   public String toString() {
      return "XSiteStateTransferClearStatusCommand{" +
            "cacheName=" + cacheName +
            '}';
   }

   public void invokeLocal(XSiteStateTransferManager stateTransferManager) {
      stateTransferManager.clearStatus();
   }
}
