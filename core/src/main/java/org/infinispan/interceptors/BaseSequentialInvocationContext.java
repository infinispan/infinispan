package org.infinispan.interceptors;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.base.SequentialInterceptor;

import java.util.Collections;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;

/**
 * @author Dan Berindei
 * @since 8.0
 */
public abstract class BaseSequentialInvocationContext extends CompletableFuture<Object>
      implements InvocationContext {
   VisitableCommand command;
   List<SequentialInterceptor> interceptors;
   // If >= interceptors.length, it means we've begun executing the return handlers
   int nextInterceptor = 0;
   Deque<BiFunction<Object, Throwable, Object>> returnHandlers = new LinkedList<>();

   public BaseSequentialInvocationContext(SequentialInterceptorChain interceptorChain) {
      this.interceptors = interceptorChain != null ? interceptorChain.getInterceptors() : null;
   }

   @Override
   public VisitableCommand getCommand() {
      return command;
   }

   @Override
   public void onReturn(BiFunction<Object, Throwable, Object> returnHandler) {
      returnHandlers.addLast(returnHandler);
   }

   @Override
   public CompletableFuture<Object> shortCircuit(Object returnValue) {
      nextInterceptor = interceptors.size();
      return CompletableFuture.completedFuture(returnValue);
   }

   @Override
   public CompletableFuture<Object> forkInvocation(VisitableCommand newCommand,
                                                   BiFunction<Object, Throwable, Object> returnHandler) {
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
            CompletableFuture<Object> nextFuture = next.visitCommand(this, command);
            if (nextFuture != null) {
               // The execution will continue when the interceptor finishes
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
            Object newReturnValue = returnHandlers.removeLast().apply(returnValue, throwable);
            if (newReturnValue != null) {
               returnValue = newReturnValue;
               throwable = null;
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
