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
   public <C extends VisitableCommand> Object thenApply(InvocationContext ctx, C command, InvocationSuccessFunction<C> function) {
      return addCallback(ctx, command, function);
   }

   /**
    * After the current stage completes successfully, invoke {@code action}.
    * <p>
    * The result may be either a plain value, {@code this}, or a new {@link InvocationStage}.
    * If {@code action} throws an exception, the result {@link InvocationStage} will complete with the same exception.
    */
   public <C extends VisitableCommand> Object thenAccept(InvocationContext ctx, C command, InvocationSuccessAction<C> action) {
      return addCallback(ctx, command, action);
   }

   /**
    * After the current stage completes exceptionally, invoke {@code function} and return its result.
    * <p>
    * The result may be either a plain value, {@code this}, or a new {@link InvocationStage}.
    * If {@code function} throws an exception, the result {@link InvocationStage} will complete with the same exception.
    */
   public <C extends VisitableCommand> Object andExceptionally(InvocationContext ctx, C command,
                                  InvocationExceptionFunction<C> function) {
      return addCallback(ctx, command, function);
   }

   /**
    * After the current stage completes, invoke {@code action}.
    * <p>
    * The result may be either a plain value, {@code this}, or a new {@link InvocationStage}.
    * If {@code action} throws an exception, the result {@link InvocationStage} will complete with the same exception.
    */
   public <C extends VisitableCommand> Object andFinally(InvocationContext ctx, C command, InvocationFinallyAction<C> action) {
      return addCallback(ctx, command, action);
   }

   /**
    * After the current stage completes, invoke {@code function} and return its result.
    * <p>
    * The result may be either a plain value, {@code this}, or a new {@link InvocationStage}.
    * If {@code function} throws an exception, the result {@link InvocationStage} will complete with the same exception.
    */
   public <C extends VisitableCommand> Object andHandle(InvocationContext ctx, C command, InvocationFinallyFunction<C> function) {
      return addCallback(ctx, command, function);
   }

   /**
    * After the current stage completes, invoke {@code function} and return its result.
    * <p>
    * The result may be either a plain value, or a new {@link InvocationStage}.
    * If {@code function} throws an exception, the result {@link InvocationStage} will complete with the same exception.
    */
   public abstract <C extends VisitableCommand> Object addCallback(InvocationContext ctx, C command, InvocationCallback<C> function);

   /**
    * After the current stage completes successfully, invoke {@code function} and return its result.
    * <p>
    * The result may be either {@code this}, or a new {@link InvocationStage}.
    * If {@code function} throws an exception, the result {@link InvocationStage} will complete with the same exception.
    */
   public <C extends VisitableCommand> InvocationStage thenApplyMakeStage(InvocationContext ctx, C command,
                                             InvocationSuccessFunction<C> function) {
      return makeStage(thenApply(ctx, command, function));
   }

   /**
    * After the current stage completes successfully, invoke {@code action}.
    * <p>
    * The result may be either {@code this}, or a new {@link InvocationStage}.
    * If {@code action} throws an exception, the result {@link InvocationStage} will complete with the same exception.
    */
   public <C extends VisitableCommand> InvocationStage thenAcceptMakeStage(InvocationContext ctx, C command,
                                              InvocationSuccessAction<C> action) {
      return makeStage(thenAccept(ctx, command, action));
   }

   /**
    * After the current stage completes exceptionally, invoke {@code function} and return its result.
    * <p>
    * The result may be either {@code this}, or a new {@link InvocationStage}.
    * If {@code function} throws an exception, the result {@link InvocationStage} will complete with the same exception.
    */
   public <C extends VisitableCommand> InvocationStage andExceptionallyMakeStage(InvocationContext ctx, C command,
                                                    InvocationExceptionFunction<C> function) {
      return makeStage(andExceptionally(ctx, command, function));
   }

   /**
    * After the current stage completes, invoke {@code action}.
    * <p>
    * The result may be either {@code this}, or a new {@link InvocationStage}.
    * If {@code action} throws an exception, the result {@link InvocationStage} will complete with the same exception.
    */
   public <C extends VisitableCommand> InvocationStage andFinallyMakeStage(InvocationContext ctx, C command,
                                              InvocationFinallyAction<C> action) {
      return makeStage(andFinally(ctx, command, action));
   }

   /**
    * After the current stage completes, invoke {@code function} and return its result.
    * <p>
    * The result may be either {@code this}, or a new {@link InvocationStage}.
    * If {@code function} throws an exception, the result {@link InvocationStage} will complete with the same exception.
    */
   public <C extends VisitableCommand> InvocationStage andHandleMakeStage(InvocationContext ctx, C command,
                                             InvocationFinallyFunction<C> function) {
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

   /**
    * Overrides the return value of this {@link InvocationStage} if it is completed successfully.
    *
    * The result may be either {@code rv}, a new {@link InvocationStage} or {@code this}
    */
   public Object thenReturn(InvocationContext ctx, VisitableCommand command, Object returnValue) {
      return thenApply(ctx, command, (rCtx, rCommand, rv) -> returnValue);
   }
}
