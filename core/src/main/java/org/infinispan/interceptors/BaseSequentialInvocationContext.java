package org.infinispan.interceptors;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.context.InvocationContext;
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
public abstract class BaseSequentialInvocationContext extends CompletableFuture<Object>
      implements InvocationContext {
   private static final Log log = LogFactory.getLog(BaseSequentialInvocationContext.class);
   private static final boolean trace = log.isTraceEnabled();

   // No need for volatile, the context must be properly published to the interceptors anyway
   private VisitableCommand command;
   private final List<SequentialInterceptor> interceptors;
   // If >= interceptors.length, it means we've begun executing the return handlers
   private volatile int nextInterceptor = 0;
   private final List<BiFunction<Object, Throwable, CompletableFuture<Object>>> returnHandlers;

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
      // TODO Return a special ShortCircuit object instance and check for it in continueExecution?
      nextInterceptor = interceptors.size();
      return returnValue;
   }

   @Override
   public Object forkInvocation(VisitableCommand newCommand,
                                BiFunction<Object, Throwable, CompletableFuture<Object>> returnHandler) {
      if (trace)
         log.tracef("Forking command %s at interceptor %s", newCommand, interceptors.get(nextInterceptor));
      VisitableCommand savedCommand = command;
      int savedInterceptor = nextInterceptor;
      command = newCommand;
      onReturn((returnValue, throwable) -> handleForkReturn(newCommand, returnHandler, savedCommand,
                                                            savedInterceptor, returnValue, throwable));
      // Proceed with the next interceptor
      return null;
   }

   protected CompletableFuture<Object> handleForkReturn(VisitableCommand newCommand,
                                                        BiFunction<Object, Throwable,
                                                              CompletableFuture<Object>> returnHandler,
                                                        VisitableCommand savedCommand, int savedInterceptor,
                                                        Object returnValue, Throwable throwable) {
      if (trace)
         log.tracef("Forked command %s done at %s", newCommand, interceptors.get(savedInterceptor));
      command = savedCommand;
      nextInterceptor = savedInterceptor;
      return returnHandler.apply(returnValue, throwable);
   }

   @Override
   public CompletableFuture<Object> execute(VisitableCommand command) {
      this.command = command;
      continueExecution(null, null);
      if (trace)
         log.tracef("Got the first visit future for %s", command);
      return this;
   }

   public void continueExecution(Object returnValue, Throwable throwable) {
      if (trace)
         log.tracef("Continue execution for %s, next interceptor %s, next return handler %s", getCommand(),
                    nextInterceptor,
                    returnHandlers.isEmpty() ? "N/A" : returnHandlers.get(returnHandlers.size() - 1));
      while (nextInterceptor < interceptors.size()) {
         if (throwable != null) {
            // Got an exception, skip the rest of the interceptors and start executing the return handlers
            nextInterceptor = interceptors.size();
            break;
         }

         // An interceptor's async execution finished, continue with the rest of the chain
         SequentialInterceptor next = interceptors.get(nextInterceptor);
         nextInterceptor++;
         try {
            if (trace)
               log.tracef("Executing interceptor %s", next);
            CompletableFuture<Object> nextFuture = next.visitCommand(this, command);
            if (trace)
               log.tracef("Executed interceptor %s, got %s", next, nextFuture);
            if (nextFuture != null) {
               // The execution will continue when the interceptor finishes
               // May continue in the current thread if the future is already done
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
            if (trace)
               log.tracef("Executing return handler %s", handler);
            CompletableFuture<Object> handlerFuture = handler.apply(returnValue, throwable);
            if (trace)
               log.tracef("Executed return handler %s, got %s", handler, handlerFuture);
            if (handlerFuture != null) {
               handlerFuture.whenComplete(this::continueExecution);
               return;
            } else {
               // If the handler modified nextInterceptor, the execution must continue with that interceptor
               // TODO Forking on the return path no longer works. If we re-enable it, we should allow the
               // TODO interceptors to save the context instead of passing this to forkInvocation
               if (nextInterceptor < interceptors.size()) {
                  continueExecution(returnValue, throwable);
               }
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
         complete(returnValue);
      } else {
         completeExceptionally(throwable);
      }
   }

   public BaseSequentialInvocationContext clone() {
      try {
         return (BaseSequentialInvocationContext) super.clone();
      } catch (CloneNotSupportedException e) {
         throw new CacheException("Impossible");
      }
   }
}
