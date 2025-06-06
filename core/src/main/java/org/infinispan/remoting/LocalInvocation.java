package org.infinispan.remoting;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import org.infinispan.Cache;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.ResponseGenerator;
import org.infinispan.remoting.transport.Address;

/**
 * Simulates a remote invocation on the local node. This is needed because the transport does not redirect to itself the
 * replicable commands.
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public class LocalInvocation implements Callable<Response>, Function<Object, Response> {

   private final ResponseGenerator responseGenerator;
   private final CacheRpcCommand command;
   private final ComponentRegistry componentRegistry;
   private final Address self;

   private LocalInvocation(ResponseGenerator responseGenerator, CacheRpcCommand command,
                           ComponentRegistry componentRegistry, Address self) {
      this.responseGenerator = responseGenerator;
      this.command = command;
      this.componentRegistry = componentRegistry;
      this.self = self;
   }

   @Override
   public Response call() throws Exception {
      return CompletableFutures.await(callAsync().toCompletableFuture());
   }

   public static LocalInvocation newInstanceFromCache(Cache<?, ?> cache, CacheRpcCommand command) {
      return newInstance(ComponentRegistry.of(cache), command);
   }

   public static LocalInvocation newInstance(ComponentRegistry componentRegistry, CacheRpcCommand command) {
      ResponseGenerator responseGenerator = componentRegistry.getResponseGenerator();
      Address self = componentRegistry.getComponent(ClusteringDependentLogic.class).getAddress();
      return new LocalInvocation(responseGenerator, command, componentRegistry, self);
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      LocalInvocation that = (LocalInvocation) o;

      return command.equals(that.command);

   }

   @Override
   public int hashCode() {
      return command.hashCode();
   }

   public CompletionStage<Response> callAsync() {
      command.setOrigin(self);
      try {
         CompletionStage<?> stage= command.invokeAsync(componentRegistry);
         return stage.thenApply(this);
      } catch (Throwable throwable) {
         return CompletableFuture.failedFuture(throwable);
      }
   }

   @Override
   public Response apply(Object retVal) {
      if (retVal instanceof Response) {
         return (Response) retVal;
      } else {
         return responseGenerator.getResponse(command, retVal);
      }
   }
}
