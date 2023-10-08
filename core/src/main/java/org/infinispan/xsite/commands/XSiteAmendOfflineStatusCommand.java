package org.infinispan.xsite.commands;

import java.util.concurrent.CompletionStage;

import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.util.ByteString;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.xsite.status.TakeOfflineManager;

/**
 * Amend a sites offline status.
 *
 * @author Ryan Emerson
 * @since 11.0
 */
@ProtoTypeId(ProtoStreamTypeIds.XSITE_AMEND_OFFLINE_STATUS_COMMAND)
public class XSiteAmendOfflineStatusCommand extends BaseRpcCommand {

   public static final int COMMAND_ID = 103;

   @ProtoField(number = 2)
   final String siteName;

   @ProtoField(number = 3)
   final Integer afterFailures;

   @ProtoField(number = 4)
   final Long minTimeToWait;

   @ProtoFactory
   public XSiteAmendOfflineStatusCommand(ByteString cacheName, String siteName, Integer afterFailures, Long minTimeToWait) {
      super(cacheName);
      this.siteName = siteName;
      this.afterFailures = afterFailures;
      this.minTimeToWait = minTimeToWait;
   }

   @Override
   public CompletionStage<?> invokeAsync(ComponentRegistry registry) throws Throwable {
      TakeOfflineManager takeOfflineManager = registry.getTakeOfflineManager().running();
      takeOfflineManager.amendConfiguration(siteName, afterFailures, minTimeToWait);
      return CompletableFutures.completedNull();
   }

   @Override
   public final boolean isReturnValueExpected() {
      return false;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public String toString() {
      return "XSiteAmendOfflineStatusCommand{" +
            "siteName='" + siteName + '\'' +
            ", afterFailures=" + afterFailures +
            ", minTimeToWait=" + minTimeToWait +
            '}';
   }
}
