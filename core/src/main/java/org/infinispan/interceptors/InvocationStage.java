package org.infinispan.interceptors;

import java.util.concurrent.CompletableFuture;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.context.InvocationContext;

/**
 * A partial command invocation, either completed or in progress.
 * <p>
 * It is similar to a {@link java.util.concurrent.CompletionStage}, but it allows more callback functions
 * to be stateless by passing the context and the invoked command as parameters.
 * <p>
 * Unlike {@link java.util.concurrent.CompletionStage}, adding a callback <em>can</em> delay the completion
 * of the initial stage and change its result.
 *
 * @author Dan Berindei
 * @since 9.0
 */
public abstract class InvocationStage {
   /**
    * Wait for the invocation to complete and return its value.
    *
    * @throws Throwable Any exception raised during the invocation.
    */
   public abstract Object get() throws Throwable;

   /**
    * @return {@code true} if the invocation is complete.
    */
   public abstract boolean isDone();

   /**
    * {@link CompletableFuture} conversion.
    */
   public abstract CompletableFuture<Object> toCompletableFuture();

   /**
    * After the current stage completes successfully, invoke {@code function} and return its result.
    * <p>
    * The result may be either a plain value, {@code this}, or a new {@link InvocationStage}.
    * If {@code function} throws an exception, the result {@link InvocationStage} will complete with the same exception.
    */
   public Object thenApply(InvocationContext ctx, VisitableCommand command, InvocationSuccessFunction function) {
      return addCallback(ctx, command, function);
   }

   /**
    * After the current stage completes successfully, invoke {@code action}.
    * <p>
    * The result may be either a plain value, {@code this}, or a new {@link InvocationStage}.
    * If {@code action} throws an exception, the result {@link InvocationStage} will complete with the same exception.
    */
   public Object thenAccept(InvocationContext ctx, VisitableCommand command, InvocationSuccessAction action) {
      return addCallback(ctx, command, action);
   }

   /**
    * After the current stage completes exceptionally, invoke {@code function} and return its result.
    * <p>
    * The result may be either a plain value, {@code this}, or a new {@link InvocationStage}.
    * If {@code function} throws an exception, the result {@link InvocationStage} will complete with the same exception.
    */
   public Object andExceptionally(InvocationContext ctx, VisitableCommand command,
                                  InvocationExceptionFunction function) {
      return addCallback(ctx, command, function);
   }

   /**
    * After the current stage completes, invoke {@code action}.
    * <p>
    * The result may be either a plain value, {@code this}, or a new {@link InvocationStage}.
    * If {@code action} throws an exception, the result {@link InvocationStage} will complete with the same exception.
    */
   public Object andFinally(InvocationContext ctx, VisitableCommand command, InvocationFinallyAction action) {
      return addCallback(ctx, command, action);
   }

   /**
    * After the current stage completes, invoke {@code function} and return its result.
    * <p>
    * The result may be either a plain value, {@code this}, or a new {@link InvocationStage}.
    * If {@code function} throws an exception, the result {@link InvocationStage} will complete with the same exception.
    */
   public Object andHandle(InvocationContext ctx, VisitableCommand command, InvocationFinallyFunction function) {
      return addCallback(ctx, command, function);
   }

   /**
    * After the current stage completes, invoke {@code function} and return its result.
    * <p>
    * The result may be either a plain value, or a new {@link InvocationStage}.
    * If {@code function} throws an exception, the result {@link InvocationStage} will complete with the same exception.
    */
   public abstract Object addCallback(InvocationContext ctx, VisitableCommand command, InvocationCallback function);

   /**
    * After the current stage completes successfully, invoke {@code function} and return its result.
    * <p>
    * The result may be either {@code this}, or a new {@link InvocationStage}.
    * If {@code function} throws an exception, the result {@link InvocationStage} will complete with the same exception.
    */
   public InvocationStage thenApplyMakeStage(InvocationContext ctx, VisitableCommand command,
                                             InvocationSuccessFunction function) {
      return makeStage(thenApply(ctx, command, function));
   }

   /**
    * After the current stage completes successfully, invoke {@code action}.
    * <p>
    * The result may be either {@code this}, or a new {@link InvocationStage}.
    * If {@code action} throws an exception, the result {@link InvocationStage} will complete with the same exception.
    */
   public InvocationStage thenAcceptMakeStage(InvocationContext ctx, VisitableCommand command,
                                              InvocationSuccessAction action) {
      return makeStage(thenAccept(ctx, command, action));
   }

   /**
    * After the current stage completes exceptionally, invoke {@code function} and return its result.
    * <p>
    * The result may be either {@code this}, or a new {@link InvocationStage}.
    * If {@code function} throws an exception, the result {@link InvocationStage} will complete with the same exception.
    */
   public InvocationStage andExceptionallyMakeStage(InvocationContext ctx, VisitableCommand command,
                                                    InvocationExceptionFunction function) {
      return makeStage(andExceptionally(ctx, command, function));
   }

   /**
    * After the current stage completes, invoke {@code action}.
    * <p>
    * The result may be either {@code this}, or a new {@link InvocationStage}.
    * If {@code action} throws an exception, the result {@link InvocationStage} will complete with the same exception.
    */
   public InvocationStage andFinallyMakeStage(InvocationContext ctx, VisitableCommand command,
                                              InvocationFinallyAction action) {
      return makeStage(andFinally(ctx, command, action));
   }

   /**
    * After the current stage completes, invoke {@code function} and return its result.
    * <p>
    * The result may be either {@code this}, or a new {@link InvocationStage}.
    * If {@code function} throws an exception, the result {@link InvocationStage} will complete with the same exception.
    */
   public InvocationStage andHandleMakeStage(InvocationContext ctx, VisitableCommand command,
                                             InvocationFinallyFunction function) {
      return makeStage(andHandle(ctx, command, function));
   }

   /**
    * If {@code maybeStage} is not an {@code InvocationStage}, wrap it, otherwise cast it to an {@code InvocationStage}.
    */
   public static InvocationStage makeStage(Object maybeStage) {
      if (maybeStage instanceof InvocationStage) {
         return (InvocationStage) maybeStage;
      } else {
         return new SyncInvocationStage(maybeStage);
      }
   }

   /**
    * @return an {@code InvocationStage} instance completed successfully with value {@code null}.
    */
   public static InvocationStage completedNullStage() {
      return SyncInvocationStage.COMPLETED_NULL_STAGE;
   }
}
