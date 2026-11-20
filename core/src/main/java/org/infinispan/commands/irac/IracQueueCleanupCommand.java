package org.infinispan.commands.irac;

import static java.util.concurrent.CompletableFuture.completedFuture;

import java.util.concurrent.CompletionStage;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.remoting.transport.NodeVersion;
import org.infinispan.util.ByteString;
import org.infinispan.xsite.irac.IracManager;
import org.infinispan.xsite.irac.IracQueueCleanupStatus;

/**
 * Starts, cancels or reads the status of the IRAC queue cleanup in a node.
 *
 * @since 16.4
 */
@ProtoTypeId(ProtoStreamTypeIds.IRAC_QUEUE_CLEANUP_COMMAND)
public class IracQueueCleanupCommand extends BaseIracCommand {

   public static final byte STATUS = 0;
   public static final byte START = 1;
   public static final byte CANCEL = 2;

   @ProtoField(2)
   final byte action;

   @ProtoField(3)
   final long minAgeMillis;

   @ProtoFactory
   public IracQueueCleanupCommand(ByteString cacheName, byte action, long minAgeMillis) {
      super(cacheName);
      this.action = action;
      this.minAgeMillis = minAgeMillis;
   }

   @Override
   public CompletionStage<IracQueueCleanupStatus> invokeAsync(ComponentRegistry registry) {
      return completedFuture(invokeLocal(registry.getIracManager().running()));
   }

   public IracQueueCleanupStatus invokeLocal(IracManager iracManager) {
      return switch (action) {
         case START -> iracManager.startQueueCleanup(minAgeMillis);
         case CANCEL -> iracManager.cancelQueueCleanup();
         default -> iracManager.queueCleanupStatus();
      };
   }

   @Override
   public boolean isReturnValueExpected() {
      return true;
   }

   @Override
   public NodeVersion supportedSince() {
      return NodeVersion.SIXTEEN_FOUR;
   }

   @Override
   public String toString() {
      return "IracQueueCleanupCommand{" +
            "cacheName=" + cacheName +
            ", action=" + action +
            ", minAgeMillis=" + minAgeMillis +
            '}';
   }
}
