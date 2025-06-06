package org.infinispan.manager.impl;

import java.util.concurrent.CompletionStage;

import javax.security.auth.Subject;

import org.infinispan.commands.GlobalRpcCommand;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.remoting.transport.NodeVersion;
import org.infinispan.security.Security;
import org.infinispan.util.concurrent.BlockingManager;

/**
 * Replicable Command that runs the given Runnable
 *
 * @author wburns
 * @since 8.2
 */
@ProtoTypeId(ProtoStreamTypeIds.REPLICABLE_RUNNABLE_COMMAND)
public class ReplicableRunnableCommand implements GlobalRpcCommand {

   final Runnable runnable;
   final Subject subject;

   public ReplicableRunnableCommand(Runnable runnable, Subject subject) {
      this.runnable = runnable;
      this.subject = subject;
   }

   @ProtoFactory
   ReplicableRunnableCommand(MarshallableObject<Runnable> runnable, Subject subject) {
      this.runnable = MarshallableObject.unwrap(runnable);
      this.subject = subject;
   }

   @ProtoField(1)
   MarshallableObject<Runnable> getRunnable() {
      return MarshallableObject.create(runnable);
   }

   @ProtoField(2)
   Subject getSubject() {
      return subject;
   }

   @Override
   public CompletionStage<?> invokeAsync(GlobalComponentRegistry globalComponentRegistry) throws Throwable {
      BlockingManager bm = globalComponentRegistry.getComponent(BlockingManager.class);
      return bm.supplyBlocking(() -> {
         if (subject == null) {
            runnable.run();
         } else {
            Security.doAs(subject, runnable);
         }
         return null;
      }, "replicable-runnable").toCompletableFuture();
   }

   @Override
   public boolean isReturnValueExpected() {
      return false;
   }

   @Override
   public NodeVersion supportedSince() {
      return NodeVersion.SIXTEEN;
   }
}
