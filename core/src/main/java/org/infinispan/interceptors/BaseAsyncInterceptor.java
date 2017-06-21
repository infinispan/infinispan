package org.infinispan.interceptors;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.commons.util.Experimental;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.impl.SimpleAsyncInvocationStage;

/**
 * Base class for an interceptor in the new asynchronous invocation chain.
 *
 * @author Dan Berindei
 * @since 9.0
 */
@Experimental
public abstract class BaseAsyncInterceptor implements AsyncInterceptor {
   private final InvocationSuccessFunction invokeNextFunction = (rCtx, rCommand, rv) -> invokeNext(rCtx, rCommand);

   protected Configuration cacheConfiguration;
   private AsyncInterceptor nextInterceptor;
   private DDAsyncInterceptor nextDDInterceptor;

   @Inject
   public void inject(Configuration cacheConfiguration) {
      this.cacheConfiguration = cacheConfiguration;
   }

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
    * {@link #asyncInvokeNext(InvocationContext, VisitableCommand, CompletableFuture)}.</p>
    */
   public static InvocationStage asyncValue(CompletableFuture<?> valueFuture) {
      return new SimpleAsyncInvocationStage(valueFuture);
   }

   /**
    * Suspend the invocation until {@code delay} completes, then if successful invoke the next interceptor.
    *
    * <p>If {@code delay} completes exceptionally, skip the next interceptor and continue with the exception.</p>
    *
    * <p>You need to wrap the result with {@link #makeStage(Object)} if you need to add another handler.</p>
    */
   public final Object asyncInvokeNext(InvocationContext ctx, VisitableCommand command,
                                                CompletableFuture<?> delay) {
      return asyncValue(delay).thenApply(ctx, command, invokeNextFunction);
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
                                       Collection<CompletableFuture<?>> delays) {
      if (delays == null || delays.isEmpty()) {
         return invokeNext(ctx, command);
      } else if (delays.size() == 1) {
         return asyncInvokeNext(ctx, command, delays.iterator().next());
      } else {
         return asyncInvokeNext(ctx, command,
               CompletableFuture.allOf(delays.toArray(new CompletableFuture[delays.size()])));
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

   protected static boolean isSuccessfullyDone(Object maybeStage) {
      if (maybeStage instanceof InvocationStage) {
         InvocationStage stage = (InvocationStage) maybeStage;
         return stage.isDone() && !stage.toCompletableFuture().isCompletedExceptionally();
      }
      return true;
   }
}
