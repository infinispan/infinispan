package org.infinispan.xsite.commands.remote;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.CompletionStage;

import org.infinispan.factories.ComponentRegistry;
import org.infinispan.util.ByteString;

/**
 * Controls the start and stop of receiving the cross-site state transfer.
 * <p>
 * It used when the backup strategy is set to {@link org.infinispan.configuration.cache.BackupConfiguration.BackupStrategy#SYNC}.
 *
 * @since 15.0
 */
public class XSiteStateTransferControlRequest extends XSiteCacheRequest<Void> {

   private boolean startReceive;

   public XSiteStateTransferControlRequest(ByteString cacheName, boolean startReceive) {
      super(cacheName);
      this.startReceive = startReceive;
   }

   public XSiteStateTransferControlRequest() {
      super(null);
   }

   @Override
   protected CompletionStage<Void> invokeInLocalCache(String origin, ComponentRegistry registry) {
      return registry.getBackupReceiver().running().handleStateTransferControl(origin, startReceive);
   }

   @Override
   public byte getCommandId() {
      return Ids.STATE_TRANSFER_CONTROL;
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      output.writeBoolean(startReceive);
      super.writeTo(output);
   }

   @Override
   public XSiteRequest<Void> readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      startReceive = input.readBoolean();
      return super.readFrom(input);
   }
}
