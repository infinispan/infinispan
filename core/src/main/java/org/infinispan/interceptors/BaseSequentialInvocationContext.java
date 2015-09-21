package org.infinispan.interceptors;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.base.SequentialInterceptor;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.ArrayList;
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
      this.interceptors = interceptorChain != null ? interceptorChain.getInterceptors() : null;
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
      VisitableCommand savedCommand = command;
      int savedInterceptor = nextInterceptor;
      command = newCommand;
      onReturn((returnValue, throwable) -> {
         command = savedCommand;
         nextInterceptor = savedInterceptor;
         return returnHandler.apply(returnValue, throwable);
      });
      // Proceed with the next interceptor
      return null;
   }

   @Override
   public CompletableFuture<Object> execute(VisitableCommand command) {
      this.command = command;
      continueExecution(null, null);
      return this;
   }

   public void continueExecution(Object returnValue, Throwable throwable) {
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

      // Interceptors are all done, execute the return handlers synchronously
      while (!returnHandlers.isEmpty()) {
         try {
            BiFunction<Object, Throwable, CompletableFuture<Object>> handler =
                  returnHandlers.remove(returnHandlers.size() - 1);
            CompletableFuture<Object> handlerFuture = handler.apply(returnValue, throwable);
            // If the handler modified nextInterceptor, the execution must continue with that interceptor
            if (handlerFuture != null) {
               handlerFuture.whenComplete(this::continueExecution);
               return;
            } else {
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
