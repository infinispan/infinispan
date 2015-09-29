package org.infinispan.context.impl;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.SequentialInterceptorChain;
import org.infinispan.interceptors.base.SequentialInterceptor;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;

/**
 * @author Dan Berindei
 * @since 8.0
 */
public abstract class BaseSequentialInvocationContext
      implements InvocationContext {
   private static final Log log = LogFactory.getLog(BaseSequentialInvocationContext.class);
   private static final boolean trace = log.isTraceEnabled();
   private static final Object SHORT_CIRCUIT_NULL = new Object();

   // No need for volatile, the context must be properly published to the interceptors anyway
   private VisitableCommand command;
   private final List<SequentialInterceptor> interceptors;
   // If >= interceptors.length, it means we've begun executing the return handlers
   private volatile int nextInterceptor = 0;
   private final List<BiFunction<Object, Throwable, CompletableFuture<Object>>> returnHandlers;
   private CompletableFuture<Object> future;

   public BaseSequentialInvocationContext(SequentialInterceptorChain interceptorChain) {
      this.interceptors = interceptorChain != null ? interceptorChain.getInterceptors() :
            Collections.emptyList();
      this.returnHandlers = new ArrayList<>(interceptors.size());
   }

   @Override
   public VisitableCommand getCommand() {
      return command;
   }

   @Override
   public void onReturn(BiFunction<Object, Throwable, CompletableFuture<Object>> returnHandler) {
      returnHandlers.add(returnHandler);
   }

   @Override
   public Object shortCircuit(Object returnValue) {
      return returnValue != null ? returnValue : SHORT_CIRCUIT_NULL;
   }

   @Override
   public Object forkInvocation(VisitableCommand newCommand,
                                BiFunction<Object, Throwable, CompletableFuture<Object>> returnHandler) {
      if (trace)
         log.tracef("Forking command %s at interceptor %d %s", newCommand, nextInterceptor, interceptors.get(nextInterceptor));
      return new ForkInfo(newCommand, returnHandler);
   }

   protected CompletableFuture<Object> handleForkReturn(VisitableCommand newCommand,
                                                        BiFunction<Object, Throwable,
                                                              CompletableFuture<Object>> returnHandler,
                                                        VisitableCommand savedCommand, int savedInterceptor,
                                                        Object returnValue, Throwable throwable) {
      if (trace)
         log.tracef("Forked command %s done at %s, return value %s/%s, continuing with %s", newCommand,
                    interceptors.get(savedInterceptor), returnValue, throwable, savedCommand);
      command = savedCommand;
      nextInterceptor = savedInterceptor;
      return returnHandler.apply(returnValue, throwable);
   }

   @Override
   public CompletableFuture<Object> execute(VisitableCommand command) {
      this.future = new CompletableFuture<>();
      this.command = command;
      this.nextInterceptor = 0;
      continueExecution(null, null);
      if (trace)
         log.tracef("Got the first visit future for %s", command);
      return future;
   }

   public void continueExecution(Object returnValue, Throwable throwable) {
      if (trace)
         log.tracef(
               "Continue execution for %s, next interceptor %s, return handlers %s, return value %s, exception %s",
               getCommand(),
               nextInterceptor, returnHandlers.size(), returnValue, throwable);
      while (nextInterceptor < interceptors.size()) {
         if (returnValue instanceof ForkInfo) {
            // Start invoking a new command with the next interceptor.
            // Save the current command and interceptor in a lambda and restore it when the forked command
            // returns.
            VisitableCommand savedCommand = command;
            int savedInterceptor = nextInterceptor;
            ForkInfo forkInfo = (ForkInfo) returnValue;
            command = forkInfo.newCommand;
            onReturn((v, t) -> handleForkReturn(savedCommand, forkInfo.returnHandler, savedCommand,
                                                savedInterceptor, v, t));
            // Proceed with the next interceptor
         } else if (returnValue != null || throwable != null) {
            // Got an exception or a short-circuit
            // Skip the rest of the interceptors and start executing the return handlers
            nextInterceptor = interceptors.size();
            if (returnValue == SHORT_CIRCUIT_NULL) {
               returnValue = null;
            }
            break;
         }

         // An interceptor's async execution finished, continue with the rest of the chain
         SequentialInterceptor next = interceptors.get(nextInterceptor);
         nextInterceptor++;
         try {
            CompletableFuture<Object> nextFuture = next.visitCommand(this, command);
            if (nextFuture != null) {
               // The execution will continue when the interceptor finishes
               // May continue in the current thread if the future is already done
               // TODO Optimize when the future is already done?
               nextFuture.whenComplete(this::continueExecution);
               return;
            }
         } catch (Throwable t) {
            throwable = t;
         }
      }

      // Interceptors are all done, execute the return handlers
      while (!returnHandlers.isEmpty()) {
         try {
            BiFunction<Object, Throwable, CompletableFuture<Object>> handler =
                  returnHandlers.remove(returnHandlers.size() - 1);
            CompletableFuture<Object> handlerFuture = handler.apply(returnValue, throwable);
            if (handlerFuture != null) {
               handlerFuture.whenComplete(this::continueExecution);
               return;
            }
         } catch (Throwable t) {
            returnValue = null;
            throwable = t;
         }
      }

      // We are done!
      if (trace)
         log.tracef("Command %s done, return value %s, throwable %s", command, returnValue, throwable);
      if (throwable == null) {
         future.complete(returnValue);
      } else {
         future.completeExceptionally(throwable);
      }
   }

   public BaseSequentialInvocationContext clone() {
      try {
         BaseSequentialInvocationContext clone = (BaseSequentialInvocationContext) super.clone();
         return clone;
      } catch (CloneNotSupportedException e) {
         throw new CacheException("Impossible");
      }
   }

   private class ForkInfo {
      private final VisitableCommand newCommand;
      private final BiFunction<Object, Throwable, CompletableFuture<Object>> returnHandler;

      public ForkInfo(VisitableCommand newCommand,
                      BiFunction<Object, Throwable, CompletableFuture<Object>> returnHandler) {
         this.newCommand = newCommand;
         this.returnHandler = returnHandler;
      }
   }
}
