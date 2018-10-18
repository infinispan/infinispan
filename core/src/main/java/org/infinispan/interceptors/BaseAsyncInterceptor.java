package org.infinispan.interceptors;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.commons.util.Experimental;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.impl.SimpleAsyncInvocationStage;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.concurrent.CompletionStages;

/**
 * Base class for an interceptor in the new asynchronous invocation chain.
 *
 * @author Dan Berindei
 * @since 9.0
 */
@Experimental
public abstract class BaseAsyncInterceptor implements AsyncInterceptor {
   private final InvocationSuccessFunction invokeNextFunction = (rCtx, rCommand, rv) -> invokeNext(rCtx, rCommand);

   @Inject protected Configuration cacheConfiguration;
   private AsyncInterceptor nextInterceptor;
   private DDAsyncInterceptor nextDDInterceptor;

   /**
    * Used internally to set up the interceptor.
    */
   @Override
   public final void setNextInterceptor(AsyncInterceptor nextInterceptor) {
      this.nextInterceptor = nextInterceptor;
      this.nextDDInterceptor =
            nextInterceptor instanceof DDAsyncInterceptor ? (DDAsyncInterceptor) nextInterceptor : null;
   }

   /**
    * Invoke the next interceptor, possibly with a new command.
    *
    * <p>Use {@link #invokeNextThenApply(InvocationContext, VisitableCommand, InvocationSuccessFunction)}
    * or {@link #invokeNextThenAccept(InvocationContext, VisitableCommand, InvocationSuccessAction)} instead
    * if you need to process the return value of the next interceptor.</p>
    *
    * <p>Note: {@code invokeNext(ctx, command)} does not throw exceptions. In order to handle exceptions from the
    * next interceptors, you <em>must</em> use
    * {@link #invokeNextAndHandle(InvocationContext, VisitableCommand, InvocationFinallyFunction)},
    * {@link #invokeNextAndFinally(InvocationContext, VisitableCommand, InvocationFinallyAction)},
    * or {@link #invokeNextAndExceptionally(InvocationContext, VisitableCommand, InvocationExceptionFunction)}.</p>
    */
   public final Object invokeNext(InvocationContext ctx, VisitableCommand command) {
      try {
         if (nextDDInterceptor != null) {
            return command.acceptVisitor(ctx, nextDDInterceptor);
         } else {
            return nextInterceptor.visitCommand(ctx, command);
         }
      } catch (Throwable throwable) {
         return new SimpleAsyncInvocationStage(throwable);
      }
   }

   /**
    * Invoke the next interceptor, possibly with a new command, and execute an {@link InvocationCallback}
    * after all the interceptors have finished successfully.
    *
    * <p>You need to wrap the result with {@link #makeStage(Object)} if you need to add another handler.</p>
    */
   public final Object invokeNextThenApply(InvocationContext ctx, VisitableCommand command,
                                           InvocationSuccessFunction function) {
      try {
         Object rv;
         if (nextDDInterceptor != null) {
            rv = command.acceptVisitor(ctx, nextDDInterceptor);
         } else {
            rv = nextInterceptor.visitCommand(ctx, command);
         }
         if (rv instanceof InvocationStage) {
            return ((InvocationStage) rv).thenApply(ctx, command, function);
         }
         return function.apply(ctx, command, rv);
      } catch (Throwable throwable) {
         return new SimpleAsyncInvocationStage(throwable);
      }
   }

   /**
    * Invoke the next interceptor, possibly with a new command, and execute an {@link InvocationCallback}
    * after all the interceptors have finished successfully.
    *
    * <p>You need to wrap the result with {@link #makeStage(Object)} if you need to add another handler.</p>
    */
   public final Object invokeNextThenAccept(InvocationContext ctx, VisitableCommand command,
                                           InvocationSuccessAction action) {
      try {
         Object rv;
         if (nextDDInterceptor != null) {
            rv = command.acceptVisitor(ctx, nextDDInterceptor);
         } else {
            rv = nextInterceptor.visitCommand(ctx, command);
         }
         if (rv instanceof InvocationStage) {
            return ((InvocationStage) rv).thenAccept(ctx, command, action);
         }
         action.accept(ctx, command, rv);
         return rv;
      } catch (Throwable throwable) {
         return new SimpleAsyncInvocationStage(throwable);
      }
   }

   /**
    * Invoke the next interceptor, possibly with a new command, and execute an {@link InvocationCallback}
    * after all the interceptors have finished with an exception.
    *
    * <p>You need to wrap the result with {@link #makeStage(Object)} if you need to add another handler.</p>
    */
   public final Object invokeNextAndExceptionally(InvocationContext ctx, VisitableCommand command,
                                                  InvocationExceptionFunction function) {
      try {
         Object rv;
         if (nextDDInterceptor != null) {
            rv = command.acceptVisitor(ctx, nextDDInterceptor);
         } else {
            rv = nextInterceptor.visitCommand(ctx, command);
         }
         if (rv instanceof InvocationStage) {
            return ((InvocationStage) rv).andExceptionally(ctx, command, function);
         }
         // No exception
         return rv;
      } catch (Throwable throwable) {
         return new SimpleAsyncInvocationStage(throwable);
      }
   }

   /**
    * Invoke the next interceptor, possibly with a new command, and execute an {@link InvocationCallback}
    * after all the interceptors have finished, with or without an exception.
    *
    * <p>You need to wrap the result with {@link #makeStage(Object)} if you need to add another handler.</p>
    */
   public final Object invokeNextAndFinally(InvocationContext ctx, VisitableCommand command,
                                            InvocationFinallyAction action) {
      try {
         Object rv;
         Throwable throwable;
         try {
            if (nextDDInterceptor != null) {
               rv = command.acceptVisitor(ctx, nextDDInterceptor);
            } else {
               rv = nextInterceptor.visitCommand(ctx, command);
            }
            throwable = null;

            if (rv instanceof InvocationStage) {
               return ((InvocationStage) rv).andFinally(ctx, command, action);
            }
         } catch (Throwable t) {
            rv = null;
            throwable = t;
         }
         action.accept(ctx, command, rv, throwable);
         return throwable == null ? rv : new SimpleAsyncInvocationStage(throwable);
      } catch (Throwable t) {
         return new SimpleAsyncInvocationStage(t);
      }
   }

   /**
    * Invoke the next interceptor, possibly with a new command, and execute an {@link InvocationCallback}
    * after all the interceptors have finished, with or without an exception.
    *
    * <p>You need to wrap the result with {@link #makeStage(Object)} if you need to add another handler.</p>
    */
   public final Object invokeNextAndHandle(InvocationContext ctx, VisitableCommand command,
                                           InvocationFinallyFunction function) {
      try {
         Object rv;
         Throwable throwable;
         try {
            if (nextDDInterceptor != null) {
               rv = command.acceptVisitor(ctx, nextDDInterceptor);
            } else {
               rv = nextInterceptor.visitCommand(ctx, command);
            }
            throwable = null;

            if (rv instanceof InvocationStage) {
               return ((InvocationStage) rv).andHandle(ctx, command, function);
            }
         } catch (Throwable t) {
            rv = null;
            throwable = t;
         }
         return function.apply(ctx, command, rv, throwable);
      } catch (Throwable throwable) {
         return new SimpleAsyncInvocationStage(throwable);
      }
   }

   /**
    * Suspend the invocation until {@code valueFuture} completes, then return its result without running
    * the remaining interceptors.
    *
    * <p>The caller can add a callback that will run when {@code valueFuture} completes, e.g.
    * {@code asyncValue(v).thenApply(ctx, command, (rCtx, rCommand, rv, t) -> invokeNext(rCtx, rCommand))}.
    * For this particular scenario, however, it's simpler to use
    * {@link #asyncInvokeNext(InvocationContext, VisitableCommand, CompletionStage)}.</p>
    */
   public static InvocationStage asyncValue(CompletionStage<?> valueFuture) {
      return new SimpleAsyncInvocationStage(valueFuture);
   }

   /**
    * Suspend the invocation until {@code delay} completes, then if successful invoke the next interceptor.
    *
    * <p>If {@code delay} is null or already completed normally, immediately invoke the next interceptor in this thread.</p>
    *
    * <p>If {@code delay} completes exceptionally, skip the next interceptor and continue with the exception.</p>
    *
    * <p>You need to wrap the result with {@link #makeStage(Object)} if you need to add another handler.</p>
    */
   public final Object asyncInvokeNext(InvocationContext ctx, VisitableCommand command,
                                       CompletionStage<?> delay) {
      if (delay == null || CompletionStages.isCompletedSuccessfully(delay)) {
         return invokeNext(ctx, command);
      }
      return asyncValue(delay).thenApply(ctx, command, invokeNextFunction);
   }

   /**
    * Suspend the invocation until {@code invocationStage} completes, then if successful invoke the next interceptor.
    *
    * <p>If {@code invocationStage} completes exceptionally, skip the next interceptor and continue with the exception.</p>
    *
    * <p>You need to wrap the result with {@link #makeStage(Object)} if you need to add another handler.</p>
    */
   public final Object asyncInvokeNext(InvocationContext ctx, VisitableCommand command,
                                       InvocationStage invocationStage) {
      return invocationStage.thenApply(ctx, command, invokeNextFunction);
   }

   /**
    * Suspend invocation until all {@code delays} complete, then if successful invoke the next interceptor.
    * If the list is empty or null, invoke the next interceptor immediately.
    *
    * <p>If any of {@code delays} completes exceptionally, skip the next interceptor and continue with the exception.</p>
    *
    * <p>You need to wrap the result with {@link #makeStage(Object)} if you need to add another handler.</p>
    */
   public final Object asyncInvokeNext(InvocationContext ctx, VisitableCommand command,
                                       Collection<? extends CompletionStage<?>> delays) {
      if (delays == null || delays.isEmpty()) {
         return invokeNext(ctx, command);
      } else if (delays.size() == 1) {
         return asyncInvokeNext(ctx, command, delays.iterator().next());
      } else {
         CompletableFuture<Void> delay = CompletableFuture.allOf(delays.stream()
                                                                       .map(CompletionStage::toCompletableFuture)
                                                                       .toArray(CompletableFuture[]::new));
         return asyncInvokeNext(ctx, command, delay);
      }
   }

   /**
    * Return the value if {@code throwable != null}, throw the exception otherwise.
    */
   public static Object valueOrException(Object rv, Throwable throwable) throws Throwable {
      if (throwable == null) {
         return rv;
      } else {
         throw throwable;
      }
   }

   /**
    * Encode the result of an {@link #invokeNext(InvocationContext, VisitableCommand)} in an {@link InvocationStage}.
    *
    * <p>May not create a new instance, if the result is already an {@code InvocationStage}.
    */
   public static InvocationStage makeStage(Object rv) {
      if (rv instanceof InvocationStage) {
         return (InvocationStage) rv;
      } else {
         return new SyncInvocationStage(rv);
      }
   }

   /**
    * Returns an InvocationStage if the provided CompletionStage is null, not completed or completed via exception.
    * If these are not true the sync value is returned directly.
    * @param stage wait for completion of this if not null
    * @param syncValue sync value to return if stage is complete or as stage value
    * @return invocation stage or sync value
    */
   public static Object delayedValue(CompletionStage<Void> stage, Object syncValue) {
      if (stage != null) {
         CompletableFuture<?> future = stage.toCompletableFuture();
         if (!future.isDone()) {
            return asyncValue(stage.thenApply(v -> syncValue));
         }
         if (future.isCompletedExceptionally()) {
            return asyncValue(stage);
         }
      }
      return syncValue;
   }

   /**
    * This method should be used instead of {@link #delayedValue(CompletionStage, Object)} when a
    * {@link InvocationFinallyFunction} is used to properly handle the exception if any is present.
    * @param stage
    * @param syncValue
    * @param throwable
    * @return
    */
   public static Object delayedValue(CompletionStage<Void> stage, Object syncValue, Throwable throwable) {
      if (throwable == null) {
         return delayedValue(stage, syncValue);
      }
      if (stage != null) {
         CompletableFuture<?> future = stage.toCompletableFuture();
         if (!future.isDone() || future.isCompletedExceptionally()) {
            return asyncValue(
                  stage.handle((ignore, t) -> {
                     if (t != null) {
                        throwable.addSuppressed(t);
                     }
                     return null;
                  }).thenCompose(ignore -> CompletableFutures.completedExceptionFuture(throwable))
            );
         }
      }
      return new SimpleAsyncInvocationStage(throwable);
   }

   /**
    * The same as {@link #delayedValue(CompletionStage, Object)}, except that it is optimizes cases where the return
    * value is null.
    * @param stage wait for completion of this if not null
    * @return invocation stage or null sync value
    */
   public static Object delayedNull(CompletionStage<Void> stage) {
      // If stage was null - meant we didn't notify or if it already completed, no reason to create a stage instance
      if (stage == null || CompletionStages.isCompletedSuccessfully(stage)) {
         return null;
      } else {
         return asyncValue(stage);
      }
   }

   protected static boolean isSuccessfullyDone(Object maybeStage) {
      if (maybeStage instanceof InvocationStage) {
         InvocationStage stage = (InvocationStage) maybeStage;
         return stage.isDone() && !stage.toCompletableFuture().isCompletedExceptionally();
      }
      return true;
   }
}
