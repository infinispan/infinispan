package org.infinispan.xsite.commands;

import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.util.ByteString;
import org.infinispan.xsite.statetransfer.XSiteStateConsumer;
import org.infinispan.xsite.statetransfer.XSiteStateTransferManager;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.CompletionStage;

/**
 * Start receiving XSite state.
 *
 * @author Ryan Emerson
 * @since 11.0
 */
public class XSiteStateTransferStartReceiveCommand extends BaseRpcCommand {

   public static final byte COMMAND_ID = 106;

   private String siteName;

   // For CommandIdUniquenessTest only
   @SuppressWarnings("unused")
   public XSiteStateTransferStartReceiveCommand() {
      this(null, null);
   }

   public XSiteStateTransferStartReceiveCommand(ByteString cacheName) {
      this(cacheName, null);
   }

   public XSiteStateTransferStartReceiveCommand(ByteString cacheName, String siteName) {
      super(cacheName);
      this.siteName = siteName;
   }

   @Override
   public CompletionStage<?> invokeAsync(ComponentRegistry registry) {
      XSiteStateTransferManager stateTransferManager = registry.getXSiteStateTransferManager().running();
      XSiteStateConsumer consumer = stateTransferManager.getStateConsumer();
      consumer.startStateTransfer(siteName);
      return CompletableFutures.completedNull();
   }

   public void setSiteName(String siteName) {
      this.siteName = siteName;
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
   public void writeTo(ObjectOutput output) throws IOException {
      MarshallUtil.marshallString(siteName, output);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      siteName = MarshallUtil.unmarshallString(input);
   }

   @Override
   public String toString() {
      return "XSiteStateTransferStartReceiveCommand{" +
            "siteName='" + siteName + '\'' +
            ", cacheName=" + cacheName +
            '}';
   }
}
