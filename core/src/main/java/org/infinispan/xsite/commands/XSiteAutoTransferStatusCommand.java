package org.infinispan.xsite.commands;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.configuration.cache.XSiteStateTransferMode;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.util.ByteString;
import org.infinispan.xsite.response.AutoStateTransferResponse;
import org.infinispan.xsite.status.SiteState;

/**
 * A {@link CacheRpcCommand} to check the remote site status and state transfer mode in the local cluster.
 *
 * @author Pedro Ruivo
 * @since 12.1
 */
public class XSiteAutoTransferStatusCommand extends BaseRpcCommand {

   public static final int COMMAND_ID = 35;

   private String site;

   @SuppressWarnings("unused") //for CommandIdUniquenessTest
   public XSiteAutoTransferStatusCommand() {
      super(null);
   }

   public XSiteAutoTransferStatusCommand(ByteString cacheName) {
      super(cacheName);
   }

   public XSiteAutoTransferStatusCommand(ByteString cacheName, String site) {
      super(cacheName);
      this.site = site;
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
   public void writeTo(ObjectOutput output) throws IOException {
      output.writeUTF(site);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      site = input.readUTF();
   }

   @Override
   public CompletionStage<AutoStateTransferResponse> invokeAsync(ComponentRegistry registry) throws Throwable {
      boolean offline = registry.getTakeOfflineManager().running().getSiteState(site) == SiteState.OFFLINE;
      XSiteStateTransferMode mode = registry.getXSiteStateTransferManager().running().stateTransferMode(site);
      return CompletableFuture.completedFuture(new AutoStateTransferResponse(offline, mode));
   }

   @Override
   public String toString() {
      return "XSiteAutoTransferStatusCommand{" +
            "cacheName=" + cacheName +
            ", site='" + site + '\'' +
            '}';
   }
}
