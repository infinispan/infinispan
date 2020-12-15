package org.infinispan.xsite.commands;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.CompletionStage;

import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.util.ByteString;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.xsite.BackupReceiver;
import org.infinispan.xsite.XSiteReplicateCommand;
import org.infinispan.xsite.statetransfer.XSiteStateConsumer;

/**
 * Finish receiving XSite state.
 *
 * @author Ryan Emerson
 * @since 11.0
 */
public class XSiteStateTransferFinishReceiveCommand extends XSiteReplicateCommand<Void> {

   public static final byte COMMAND_ID = 107;

   private String siteName;

   // For CommandIdUniquenessTest only
   public XSiteStateTransferFinishReceiveCommand() {
      super(COMMAND_ID, null);
   }

   public XSiteStateTransferFinishReceiveCommand(ByteString cacheName) {
      this(cacheName, null);
   }

   public XSiteStateTransferFinishReceiveCommand(ByteString cacheName, String siteName) {
      super(COMMAND_ID, cacheName);
      this.siteName = siteName;
   }

   @Override
   public CompletionStage<?> invokeAsync(ComponentRegistry registry) {
      invokeLocal(registry.getXSiteStateTransferManager().running().getStateConsumer());
      return CompletableFutures.completedNull();
   }

   @Override
   public CompletionStage<Void> performInLocalSite(BackupReceiver receiver, boolean preserveOrder) {
      assert !preserveOrder;
      return receiver.handleEndReceivingStateTransfer(this);
   }

   public void setSiteName(String siteName) {
      this.siteName = siteName;
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      MarshallUtil.marshallString(siteName, output);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      siteName = MarshallUtil.unmarshallString(input);
   }

   public static XSiteStateTransferFinishReceiveCommand copyForCache(XSiteStateTransferFinishReceiveCommand command, ByteString cacheName) {
      if (!command.cacheName.equals(cacheName))
         return new XSiteStateTransferFinishReceiveCommand(cacheName, command.originSite);

      command.siteName = command.originSite;
      return command;
   }

   @Override
   public String toString() {
      return "XSiteStateTransferFinishReceiveCommand{" +
            "siteName='" + siteName + '\'' +
            ", cacheName=" + cacheName +
            '}';
   }

   public void invokeLocal(XSiteStateConsumer consumer) {
      consumer.endStateTransfer(siteName);
   }
}
