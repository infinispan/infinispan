package org.infinispan.context;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.commons.util.Experimental;
import org.infinispan.interceptors.SequentialInterceptor;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Invocation context for the sequential interceptors.
 *
 * {@link org.infinispan.context.InvocationContext} was retrofitted to extend this interface,
 * so existing invocation contexts all implement it.
 *
 * @author Dan Berindei
 * @since 9.0
 */
public interface SequentialInvocationContext {
   /**
    * Execute a callback after the rest of the interceptors in the chain have finished executing.
    *
    * <p>Return handlers are executed in LIFO order.</p>
    *
    * <p>The return handler can change the result of the command, or the exception thrown, by returning a
    * {@code CompletableFuture<Void>} instance.
    * E.g. returning {@code CompletableFuture.completedFuture(null)} will reset any exception and set the
    * result to {@code null}.</p>
    * Returning {@code null} will preserve the previous result or exception (equivalent to a
    * {@code finally} block in an old-style interceptor).
    */
   CompletableFuture<Void> onReturn(SequentialInterceptor.ReturnHandler returnHandler);

   /**
    * Equivalent to {@code null}, when returned by
    * {@link SequentialInterceptor#visitCommand(InvocationContext, VisitableCommand)} or
    * {@link SequentialInterceptor.ReturnHandler#handle(InvocationContext, VisitableCommand, Object, Throwable)}.
    *
    * <p>Unlike {@code null}, it can be used with {@link CompletableFuture#thenCompose(Function)}.
    * And unlike {@code CompletableFuture.completedFuture(null)}, it will not allocate a new object.</p>
    */
   CompletableFuture<Void> continueInvocation();

   /**
    * Skips the remaining interceptors in the chain.
    *
    * <p>The invocation will continue with the return handlers in LIFO order.</p>
    *
    * <p>Non-fork return handlers cannot use this method, instead they can return a new value directly.</p>
    *
    * @param returnValue The value that should be returned by the command (can be modified by a return handler).
    */
   CompletableFuture<Void> shortCircuit(Object returnValue);

   /**
    * Fork the command invocation.
    *
    * <p>The rest of the interceptors in the stack, and any return handlers they add, will be executed
    * with the command {@code newCommand}.</p>
    *
    * <p>The invocation will then continue with the {@code forkHandler} callback. The return handler then
    * behaves as another {@code visitCommand} invocation: it can allow the invocation of the original command
    * to continue with the next interceptor, short-circuit the invocation, or fork another command.</p>
    *
    * @param newCommand The command to fork
    * @param forkReturnHandler The return callback
    */
   CompletableFuture<Void> forkInvocation(VisitableCommand newCommand, SequentialInterceptor.ForkReturnHandler forkReturnHandler);

   /**
    * Fork the command invocation, and execute the remaining interceptors (and their return handlers)
    * synchronously.
    *
    * <p>Usually it is easier to use than
    * {@link #forkInvocation(VisitableCommand, SequentialInterceptor.ForkReturnHandler)}.
    * However, it is not recommended, because any asynchronous work in the remaining interceptors will block
    * the calling thread.</p>
    *
    * <p>Note: This method is experimental</p>
    *
    * @param newCommand The command to fork
    */
   @Experimental
   Object forkInvocationSync(VisitableCommand newCommand) throws InterruptedException, Throwable;
}
