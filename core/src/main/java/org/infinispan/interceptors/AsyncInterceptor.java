package org.infinispan.interceptors;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.commons.util.Experimental;
import org.infinispan.context.AsyncInvocationContext;
import org.infinispan.context.InvocationContext;

import java.util.concurrent.CompletableFuture;

/**
 * Interface for sequential interceptors.
 *
 * @author Dan Berindei
 * @since 9.0
 */
@Experimental
public interface AsyncInterceptor {
   /**
    * Perform some work for the command, before the command itself is executed.
    *
    * <p>Must return an instance of {@code CompletableFuture<Void>}.</p>
    *
    * <p>Can return an already-completed {@code CompletableFuture<Void>}
    * (e.g. {@link AsyncInvocationContext#continueInvocation()}) if the interceptor is synchronous,
    * i.e. it finishes executing when {@code visitCommand} returns.</p>
    *
    * <p>The interceptor can also influence the execution of the following interceptors in the chain:</p>
    * <ul>
    * <li>The interceptor can call {@link AsyncInvocationContext#shortCircuit(Object)} in order to skip
    * the execution of the rest of the chain (and the command itself).</li>
    * <li>The interceptor can call
    * {@link AsyncInvocationContext#forkInvocation(VisitableCommand, ForkReturnHandler)} in order to invoke
    * a new command, starting with the next interceptor in the chain. The return handler then behaves as
    * another {@code visitCommand} invocation: it can allow the invocation of the original command to continue
    * with the next interceptor, short-circuit the invocation, or fork another command.</li>
    * <li>{@link AsyncInvocationContext#forkInvocationSync(VisitableCommand)} is a synchronous
    * alternative to {@code forkInvocation}. It is easier to use, however it is not recommended,
    * because any asynchronous work in the remaining interceptors will block the calling thread.</li>
    * </ul>
    *
    * <p>Thread safety: The interceptor must only invoke methods on the context or command in the thread
    * calling {@code visitCommand(InvocationContext, VisitableCommand)},
    * {@link ReturnHandler#handle(InvocationContext, VisitableCommand, Object, Throwable)}, or
    * {@link ForkReturnHandler#handle(InvocationContext, VisitableCommand, Object, Throwable)}.</p>
    */
   CompletableFuture<Void> visitCommand(InvocationContext ctx, VisitableCommand command) throws Throwable;

   /**
    * A callback executed after all the interceptors in the chain have finished.
    *
    * <p>Return handlers are executed in LIFO order.</p>
    *
    * <p>Regular return handlers can change the command's result by returning a new value wrapped in a
    * {@code CompletableFuture}. Returning {@code null} is allowed, and will continue the invocation with
    * the next return handler, without modifying the return value and without creating a new
    * {@code CompletableFuture} instance.</p>
    *
    * TODO Have different interfaces for processing the return value and for finally-like handlers that don't have access to the return value
    * TODO At least prohibit null return values and require an already-completed CF with a known value instead.
    */
   interface ReturnHandler {
      CompletableFuture<Object> handle(InvocationContext rCtx, VisitableCommand rCommand, Object rv,
            Throwable throwable) throws Throwable;
   }

   /**
    * A return handler installed with
    * {@link AsyncInvocationContext#forkInvocation(VisitableCommand, ForkReturnHandler)}.
    *
    * <p>It must behave just like {@link #visitCommand(InvocationContext, VisitableCommand)}.</p>
    */
   interface ForkReturnHandler {
      CompletableFuture<Void> handle(InvocationContext rCtx, VisitableCommand rCommand, Object rv,
            Throwable throwable) throws Throwable;
   }
}
