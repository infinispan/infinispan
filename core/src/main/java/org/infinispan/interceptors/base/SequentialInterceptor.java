package org.infinispan.interceptors.base;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.context.InvocationContext;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;

/**
 * An interceptor in the new sequential invocation chain.
 * 
 * @author Dan Berindei
 * @since 8.1
 */
public interface SequentialInterceptor extends AnyInterceptor {
   /**
    * Perform some work for the command, before the command itself is executed.
    *
    * Must return an instance of {@code CompletableFuture<Object>} if the interceptor
    * started an asynchronous task, or {@code null} if it finished its work synchronously.
    *
    * If it needs to skip the execution of the rest of the chain (and the command itself),
    * the interceptor must use {@code return skipNextInterceptor(earlyReturnValue)}, either
    * in the method or in the asynchronous task.
    * If it needs to invoke a new command, starting with the next interceptor in the chain,
    * the interceptor must use {@code return invokeNewCommand(command)}, either in the method
    * or in the asynchronous task.
    */
   CompletableFuture<Object> visitCommand(InvocationContext ctx, VisitableCommand command) throws Throwable;
}
