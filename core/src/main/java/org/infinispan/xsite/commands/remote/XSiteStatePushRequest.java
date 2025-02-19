package org.infinispan.xsite.commands.remote;

import java.util.concurrent.CompletionStage;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.util.ByteString;
import org.infinispan.xsite.statetransfer.XSiteState;

/**
 * Wraps the state to be sent to another site.
 * <p>
 * It used when the backup strategy is set to {@link org.infinispan.configuration.cache.BackupConfiguration.BackupStrategy#SYNC}.
 *
 * @since 15.0
 */
@ProtoTypeId(ProtoStreamTypeIds.XSITE_STATE_PUSH_REQUEST)
public class XSiteStatePushRequest extends XSiteCacheRequest<Void> {

   @ProtoField(2)
   final XSiteState[] chunk;
   @ProtoField(3)
   long timeoutMillis;

   @ProtoFactory
   public XSiteStatePushRequest(ByteString cacheName, XSiteState[] chunk, long timeoutMillis) {
      super(cacheName);
      this.chunk = chunk;
      this.timeoutMillis = timeoutMillis;
   }

   @Override
   protected CompletionStage<Void> invokeInLocalCache(String origin, ComponentRegistry registry) {
      return registry.getBackupReceiver().running().handleStateTransferState(chunk, timeoutMillis);
   }

   @Override
   public byte getCommandId() {
      return Ids.STATE_TRANSFER_STATE;
   }
}
