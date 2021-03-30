package org.infinispan.xsite.commands;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.CompletionStage;

import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.configuration.cache.XSiteStateTransferMode;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.util.ByteString;
import org.infinispan.util.concurrent.CompletableFutures;

/**
 * A {@link CacheRpcCommand} that sets the {@link XSiteStateTransferMode} cluster-wide.
 *
 * @author Pedro Ruivo
 * @since 12.1
 */
public class XSiteSetStateTransferModeCommand extends BaseRpcCommand {

   public static final int COMMAND_ID = 36;

   private String site;
   private XSiteStateTransferMode mode;

   @SuppressWarnings("unused") // for CommandIdUniquenessTest
   public XSiteSetStateTransferModeCommand() {
      super(null);
   }

   public XSiteSetStateTransferModeCommand(ByteString cacheName) {
      super(cacheName);
   }

   public XSiteSetStateTransferModeCommand(ByteString cacheName, String site, XSiteStateTransferMode mode) {
      super(cacheName);
      this.site = site;
      this.mode = mode;
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
      output.writeUTF(site);
      MarshallUtil.marshallEnum(mode, output);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      site = input.readUTF();
      mode = MarshallUtil.unmarshallEnum(input, XSiteStateTransferMode::valueOf);
   }

   @Override
   public CompletionStage<Void> invokeAsync(ComponentRegistry registry) throws Throwable {
      registry.getXSiteStateTransferManager().running().setAutomaticStateTransfer(site, mode);
      return CompletableFutures.completedNull();
   }
}
