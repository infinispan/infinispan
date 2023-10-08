package org.infinispan.xsite.commands.remote;

import java.util.concurrent.CompletionStage;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.util.ByteString;

/**
 * Controls the start and stop of receiving the cross-site state transfer.
 * <p>
 * It used when the backup strategy is set to {@link org.infinispan.configuration.cache.BackupConfiguration.BackupStrategy#SYNC}.
 *
 * @since 15.0
 */
@ProtoTypeId(ProtoStreamTypeIds.XSITE_STATE_TRANSFER_CONTROLLER_REQUEST)
public class XSiteStateTransferControlRequest extends XSiteCacheRequest<Void> {

   @ProtoField(number = 2, defaultValue = "false")
   final boolean startReceive;

   @ProtoFactory
   public XSiteStateTransferControlRequest(ByteString cacheName, boolean startReceive) {
      super(cacheName);
      this.startReceive = startReceive;
   }

   @Override
   protected CompletionStage<Void> invokeInLocalCache(String origin, ComponentRegistry registry) {
      return registry.getBackupReceiver().running().handleStateTransferControl(origin, startReceive);
   }

   @Override
   public byte getCommandId() {
      return Ids.STATE_TRANSFER_CONTROL;
   }
}
