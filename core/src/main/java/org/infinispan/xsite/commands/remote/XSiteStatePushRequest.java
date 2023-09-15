package org.infinispan.xsite.commands.remote;

import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.util.ByteString;
import org.infinispan.xsite.statetransfer.XSiteState;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.CompletionStage;

/**
 * Wraps the state to be sent to another site.
 * <p>
 * It used when the backup strategy is set to {@link org.infinispan.configuration.cache.BackupConfiguration.BackupStrategy#SYNC}.
 *
 * @since 15.0
 */
public class XSiteStatePushRequest extends XSiteCacheRequest<Void> {

   private XSiteState[] chunk;
   private long timeoutMillis;

   public XSiteStatePushRequest() {
      super(null);
   }

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

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      output.writeLong(timeoutMillis);
      MarshallUtil.marshallArray(chunk, output);
      super.writeTo(output);
   }

   @Override
   public XSiteRequest<Void> readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      timeoutMillis = input.readLong();
      chunk = MarshallUtil.unmarshallArray(input, XSiteState[]::new);
      return super.readFrom(input);
   }
}
