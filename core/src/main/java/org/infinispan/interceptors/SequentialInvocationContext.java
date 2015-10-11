package org.infinispan.interceptors;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.BaseSequentialInvocationContext;
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

   void onReturn(ReturnHandler returnHandler);

   Object shortCircuit(Object returnValue);

   CompletableFuture<Object> forkInvocation(VisitableCommand newCommand, ReturnHandler returnHandler);

   CompletableFuture<Object> execute(VisitableCommand command);

   // TODO Object executeRestOfChain() for legacy interceptors and/or local execution?

   interface ReturnHandler {
      CompletableFuture<Object> apply(Object returnValue, Throwable throwable) throws Throwable;
   }
}
