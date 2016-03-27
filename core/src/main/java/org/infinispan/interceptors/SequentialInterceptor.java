package org.infinispan.interceptors;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.commons.util.Experimental;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.SequentialInvocationContext;

import java.util.concurrent.CompletableFuture;

/**
 * Interface for sequential interceptors.
 *
 * @author Dan Berindei
 * @since 9.0
 */
@Experimental
public interface SequentialInterceptor {
   /**
    * Perform some work for the command, before the command itself is executed.
    *
    * <p>Must return an instance of {@code CompletableFuture<Void>}.</p>
    *
    * <p>Can return an already-completed {@code CompletableFuture<Void>}
    * (e.g. {@link SequentialInvocationContext#continueInvocation()}) if the interceptor is synchronous,
    * i.e. it finishes executing when {@code visitCommand} returns.</p>
    *
    * <p>The interceptor can also influence the execution of the following interceptors in the chain:</p>
    * <ul>
    * <li>The interceptor can call {@link SequentialInvocationContext#shortCircuit(Object)} in order to skip
    * the execution of the rest of the chain (and the command itself).</li>
    * <li>The interceptor can call
    * {@link SequentialInvocationContext#forkInvocation(VisitableCommand, ReturnHandler)} in order to invoke
    * a new command, starting with the next interceptor in the chain. The return handler then behaves as
    * another {@code visitCommand} invocation: it can allow the invocation of the original command to continue
    * with the next interceptor, short-circuit the invocation, or fork another command.</li>
    * <li>{@link SequentialInvocationContext#forkInvocationSync(VisitableCommand)} is a synchronous
    * alternative to {@code forkInvocation}. It is easier to use, however it is not recommended,
    * because any asynchronous work in the remaining interceptors will block the calling thread.</li>
    * </ul>
    *
    * <p>Thread safety: The interceptor must only invoke methods on the context or command in the thread
    * calling {@link #visitCommand(InvocationContext, VisitableCommand)} or
    * {@link ReturnHandler#handle(InvocationContext, VisitableCommand, Object, Throwable)}.</p>
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
    */
   interface ReturnHandler {
      CompletableFuture<Object> handle(InvocationContext ctx, VisitableCommand command, Object rv,
            Throwable throwable) throws Throwable;
   }

   /*
   * A return handler installed with
   * {@link SequentialInvocationContext#forkInvocation(VisitableCommand, ForkHandler)}.
    *
    * <p>It must behave just like {@link #visitCommand(InvocationContext, VisitableCommand)}.</p>
   */
   interface ForkReturnHandler {
      CompletableFuture<Void> handle(InvocationContext ctx, VisitableCommand command, Object rv,
            Throwable throwable) throws Throwable;
   }
}
