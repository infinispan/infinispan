package org.infinispan.interceptors;

import java.util.concurrent.CompletableFuture;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.context.InvocationContext;

/**
 * @author Dan Berindei
 * @since 9.0
 */
public abstract class InvocationStage {
   public abstract Object get() throws Throwable;

   public abstract boolean isDone();

   public abstract CompletableFuture<Object> toCompletableFuture();

   /**
    * After this stage completes successfully, invoke {@code function} and return either its result
    */
   public Object thenApply(InvocationContext ctx, VisitableCommand command,
                                    InvocationSuccessFunction function) {
      return addCallback(ctx, command, function);
   }

   public Object thenAccept(InvocationContext ctx, VisitableCommand command,
                                     InvocationSuccessAction function) {
      return addCallback(ctx, command, function);
   }

   public Object andExceptionally(InvocationContext ctx, VisitableCommand command,
                                  InvocationExceptionFunction function) {
      return addCallback(ctx, command, function);
   }

   public Object andFinally(InvocationContext ctx, VisitableCommand command,
                            InvocationFinallyAction action) {
      return addCallback(ctx, command, action);
   }

   public Object andHandle(InvocationContext ctx, VisitableCommand command,
                           InvocationFinallyFunction function) {
      return addCallback(ctx, command, function);
   }

   public abstract Object addCallback(InvocationContext ctx, VisitableCommand command,
                                      InvocationCallback function);
}
