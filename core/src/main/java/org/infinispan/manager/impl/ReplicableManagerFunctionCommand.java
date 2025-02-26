package org.infinispan.manager.impl;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import javax.security.auth.Subject;

import org.infinispan.commands.GlobalRpcCommand;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.security.Security;
import org.infinispan.util.concurrent.BlockingManager;

/**
 * Replicable Command that runs the given Function passing the {@link EmbeddedCacheManager} as an argument
 *
 * @author wburns
 * @since 8.2
 */
@ProtoTypeId(ProtoStreamTypeIds.REPLICABLE_MANAGER_FUNCTION_COMMAND)
@Scope(Scopes.NONE)
public class ReplicableManagerFunctionCommand implements GlobalRpcCommand {

   final Function<? super EmbeddedCacheManager, ?> function;
   final Subject subject;

   public ReplicableManagerFunctionCommand(Function<? super EmbeddedCacheManager, ?> function, Subject subject) {
      this.function = function;
      this.subject = subject;
   }

   @ProtoFactory
   ReplicableManagerFunctionCommand(MarshallableObject<Function<? super EmbeddedCacheManager, ?>> function, Subject subject) {
      this.function = MarshallableObject.unwrap(function);
      this.subject = subject;
   }

   @ProtoField(1)
   MarshallableObject<Function<? super EmbeddedCacheManager, ?>> getFunction() {
      return MarshallableObject.create(function);
   }

   @ProtoField(2)
   Subject getSubject() {
      return subject;
   }

   @Override
   public CompletableFuture<Object> invokeAsync(GlobalComponentRegistry globalComponentRegistry) throws Throwable {
      BlockingManager bm = globalComponentRegistry.getComponent(BlockingManager.class);
      return bm.supplyBlocking(() -> {
         if (subject == null) {
            return function.apply(new UnwrappingEmbeddedCacheManager(globalComponentRegistry.getCacheManager()));
         } else {
            return Security.doAs(subject, function, new UnwrappingEmbeddedCacheManager(globalComponentRegistry.getCacheManager()));
         }
      }, "replicable-manager-function").toCompletableFuture();
   }

   @Override
   public boolean isReturnValueExpected() {
      return true;
   }
}
