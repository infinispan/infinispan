package org.infinispan.manager.impl;

import java.util.concurrent.CompletionStage;

import org.infinispan.commands.GlobalRpcCommand;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * Replicable Command that runs the given Runnable
 *
 * @author wburns
 * @since 8.2
 */
@ProtoTypeId(ProtoStreamTypeIds.REPLICABLE_RUNNABLE_COMMAND)
public class ReplicableRunnableCommand implements GlobalRpcCommand {

   public static final byte COMMAND_ID = 59;

   final Runnable runnable;

   public ReplicableRunnableCommand(Runnable runnable) {
      this.runnable = runnable;
   }

   @ProtoFactory
   ReplicableRunnableCommand(MarshallableObject<Runnable> runnable) {
      this.runnable = MarshallableObject.unwrap(runnable);
   }

   @ProtoField(1)
   MarshallableObject<Runnable> getRunnable() {
      return MarshallableObject.create(runnable);
   }

   @Override
   public CompletionStage<?> invokeAsync(GlobalComponentRegistry globalComponentRegistry) throws Throwable {
      runnable.run();
      return CompletableFutures.completedNull();
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
   public boolean canBlock() {
      // These commands can be arbitrary user commands - so be careful about them blocking
      return true;
   }
}
