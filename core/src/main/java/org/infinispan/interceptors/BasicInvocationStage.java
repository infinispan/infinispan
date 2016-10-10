package org.infinispan.interceptors;

import java.util.concurrent.CompletableFuture;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.context.InvocationContext;

/**
 * A basic interface for invocation stages that can be returned by
 * {@link AsyncInterceptor#visitCommand(InvocationContext, VisitableCommand)}.
 *
 * @author Dan Berindei
 * @since 9.0
 */
public interface BasicInvocationStage {
   /**
    * @return the result of the invocation stage.
    * @throws Throwable If the invocation stage threw an exception.
    */
   Object get() throws Throwable;

   /**
    * @return {@code true} if the invocation is completed, {@code false} otherwise.
    */
   boolean isDone();

   /**
    * Convert to {@link CompletableFuture}, used internally by the asynchronous API.
    */
   CompletableFuture<Object> toCompletableFuture();

   /**
    * Convert to a full {@link InvocationStage}, used internally by the asynchronous API.
    */
   InvocationStage toInvocationStage(InvocationContext newCtx, VisitableCommand newCommand);
}
