package org.infinispan.interceptors;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.context.InvocationContext;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;

/**
 * @author Dan Berindei
 * @since 8.0
 */
public interface SequentialInvocationContext {
   VisitableCommand getCommand();

   void onReturn(BiFunction<Object, Throwable, Object> returnHandler);

   Object shortCircuit(Object returnValue);

   Object forkInvocation(VisitableCommand newCommand,
                                            BiFunction<Object, Throwable, Object> returnHandler);

   CompletableFuture<Object> execute(VisitableCommand command);
}
