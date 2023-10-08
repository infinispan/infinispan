package org.infinispan.xsite.commands;

import java.util.concurrent.CompletionStage;

import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.configuration.cache.XSiteStateTransferMode;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.util.ByteString;

/**
 * A {@link CacheRpcCommand} that sets the {@link XSiteStateTransferMode} cluster-wide.
 *
 * @author Pedro Ruivo
 * @since 12.1
 */
@ProtoTypeId(ProtoStreamTypeIds.XSITE_SET_STATE_TRANSFER_MODE_COMMAND)
public class XSiteSetStateTransferModeCommand extends BaseRpcCommand {

   public static final int COMMAND_ID = 36;

   @ProtoField(2)
   final String site;

   @ProtoField(3)
   final XSiteStateTransferMode mode;

   @ProtoFactory
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
   public CompletionStage<Void> invokeAsync(ComponentRegistry registry) throws Throwable {
      registry.getXSiteStateTransferManager().running().setAutomaticStateTransfer(site, mode);
      return CompletableFutures.completedNull();
   }
}
