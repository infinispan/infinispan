package org.infinispan.interceptors;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.base.SequentialInterceptor;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * @author Dan Berindei
 * @since 8.0
 */
public interface SequentialInvocationContext {
   VisitableCommand getCommand();

   void onReturn(BiFunction<Object, Throwable, CompletableFuture<Object>> returnHandler);

   Object shortCircuit(Object returnValue);

   Object forkInvocation(VisitableCommand newCommand, BiFunction<Object, Throwable, CompletableFuture<Object>> returnHandler);

   CompletableFuture<Object> execute(VisitableCommand command);

   // TODO Object executeRestOfChain() for legacy interceptors and/or local execution?
}
