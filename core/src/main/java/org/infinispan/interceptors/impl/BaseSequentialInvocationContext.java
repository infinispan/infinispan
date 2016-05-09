package org.infinispan.interceptors.impl;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.SequentialInvocationContext;
import org.infinispan.interceptors.SequentialInterceptor;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.jboss.logging.NDC;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

/**
 * This base class implements the {@link org.infinispan.context.SequentialInvocationContext} methods.
 *
 * @author Dan Berindei
 * @since 9.0
 */
public abstract class BaseSequentialInvocationContext
      implements InvocationContext, SequentialInvocationContext {
   private static final Log log = LogFactory.getLog(BaseSequentialInvocationContext.class);
   private static final boolean trace = log.isTraceEnabled();
   // Enable when debugging BaseSequentialInvocationContext itself
   private static final boolean EXTRA_LOGS = SecurityActions.getBooleanProperty("org.infinispan.debug.BaseSequentialInvocationContext");

   private static final CompletableFuture<Void> CONTINUE_INVOCATION = CompletableFuture.completedFuture(null);
   private static final int INVOKE_NEXT = 0;
   private static final int SHORT_CIRCUIT = 1;
   private static final int STOP_INVOCATION = 2;
   private static final int FORK_INVOCATION = 3;

   // The next interceptor to execute.
   // Note: The field is only guaranteed to be correct while an interceptor is executing,
   // in order to support forkInvocationSync.
   private InterceptorListNode nextInterceptor;
   // The next return handler to execute
   private ReturnHandlerNode nextReturnHandler;
   private CompletableFuture<Object> future;
   private int action;
   private Object actionValue;

   @Override
   public final CompletableFuture<Void> onReturn(SequentialInterceptor.ReturnHandler returnHandler) {
      nextReturnHandler = new ReturnHandlerNode(returnHandler, nextReturnHandler);
      return CONTINUE_INVOCATION;
   }

   @Override
   public final CompletableFuture<Void> continueInvocation() {
      return CONTINUE_INVOCATION;
   }

   @Override
   public final CompletableFuture<Void> shortCircuit(Object returnValue) {
      preActionCheck();
      action = SHORT_CIRCUIT;
      actionValue = returnValue;
      return CONTINUE_INVOCATION;
   }

   @Override
   public final CompletableFuture<Void> forkInvocation(VisitableCommand newCommand,
         SequentialInterceptor.ForkReturnHandler forkReturnHandler) {
      preActionCheck();
      InterceptorListNode localNode = this.nextInterceptor;
      if (localNode == null) {
         throw new IllegalStateException(
               "Cannot call shortCircuit or forkInvocation after all interceptors have executed");
      }
      this.action = FORK_INVOCATION;
      ForkInfo forkInfo = new ForkInfo(newCommand, forkReturnHandler);
      forkInfo.savedInterceptor = localNode;
      this.actionValue = forkInfo;
      return CONTINUE_INVOCATION;
   }

   private void preActionCheck() {
      if (action != INVOKE_NEXT) {
         throwActionException();
      }
   }

   private void throwActionException() {
      throw new IllegalStateException(
            "An interceptor can call shortCircuit or forkInvocation at most once. The current action is " +
                  actionName(action));
   }

   @Override
   public Object forkInvocationSync(VisitableCommand newCommand) throws Throwable {
      InterceptorListNode savedInterceptorNode = nextInterceptor;
      ReturnHandlerNode savedReturnHandler = nextReturnHandler;
      nextReturnHandler = null;
      try {
         Object returnValue = invokeInterceptorsSync(newCommand, savedInterceptorNode);
         return invokeReturnHandlersSync(newCommand, returnValue, null);
      } catch (Throwable t) {
         // Unwrap the exception from CompletableFutures.await
         Throwable throwable = extractCompletableFutureException(t);
         return invokeReturnHandlersSync(newCommand, null, throwable);
      } finally {
         action = INVOKE_NEXT;
         nextInterceptor = savedInterceptorNode;
         nextReturnHandler = savedReturnHandler;
      }
   }

   private Throwable extractCompletableFutureException(Throwable t) {
      return t instanceof ExecutionException ? t.getCause() :
             t instanceof CompletionException ? t.getCause() : t;
   }

   private Object invokeReturnHandlersSync(VisitableCommand command, Object returnValue, Throwable throwable)
         throws Throwable {
      ReturnHandlerNode returnHandlerNode = nextReturnHandler;
      nextReturnHandler = null;
      while (returnHandlerNode != null) {
         SequentialInterceptor.ReturnHandler current = returnHandlerNode.returnHandler;
         returnHandlerNode = returnHandlerNode.nextNode;

         try {
            returnValue = invokeReturnHandlerSync(current, command, returnValue, throwable);
            throwable = null;
         } catch (Throwable t) {
            throwable = t;
         }
      }
      if (throwable == null) {
         return returnValue;
      } else {
         throw throwable;
      }
   }

   final CompletableFuture<Object> invoke(VisitableCommand command, InterceptorListNode firstInterceptor) {
      future = new CompletableFuture<>();
      nextInterceptor = firstInterceptor;
      nextReturnHandler = null;
      action = INVOKE_NEXT;
      invokeNextWithContext(command, null, null);
      return future;
   }

   private void invokeNextWithContext(VisitableCommand command, Object returnValue, Throwable throwable) {
      // Populate the NDC even if TRACE is not enabled for org.infinispan.context.impl
      Object lockOwner = getLockOwner();
      if (lockOwner != null) {
         NDC.push(lockOwner.toString());
      }
      try {
         invokeNext(command, returnValue, throwable);
      } finally {
         NDC.pop();
      }
   }

   private void invokeNext(VisitableCommand command, Object returnValue, Throwable throwable) {
      InterceptorListNode interceptorNode = this.nextInterceptor;
      while (true) {
         if (action == FORK_INVOCATION) {
            // forkInvocation start
            // Start invoking a new command with the next interceptor.
            // Save the current command and interceptor, and restore them when the forked command returns.
            ForkInfo forkInfo = (ForkInfo) this.actionValue;
            forkInfo.savedCommand = command;
            command = forkInfo.newCommand;
            nextReturnHandler = new ReturnHandlerNode(forkInfo, nextReturnHandler);
         } else if (action == STOP_INVOCATION) {
            // forkInvocationSync end
            action = INVOKE_NEXT;
            return;
         } else if (action == SHORT_CIRCUIT) {
            returnValue = actionValue;
            // Skip the remaining interceptors
            interceptorNode = null;
            nextInterceptor = null;
         }
         action = INVOKE_NEXT;
         if (interceptorNode != null) {
            SequentialInterceptor interceptor = interceptorNode.interceptor;
            interceptorNode = interceptorNode.nextNode;
            nextInterceptor = interceptorNode;
            if (trace) {
               log.tracef("Executing interceptor %s with command %s", className(interceptor),
                     className(command));
            }
            try {
               CompletableFuture<Void> nextFuture = interceptor.visitCommand(this, command);
               if (nextFuture.isDone()) {
                  returnValue = nextFuture.getNow(null);
                  // continue
               } else {
                  // Some of the interceptor's processing is async
                  // The execution will continue when the interceptor finishes
                  if (EXTRA_LOGS && trace)
                     log.tracef("Interceptor %s continues asynchronously", interceptor);
                  final VisitableCommand finalCommand = command;
                  nextFuture.whenComplete(
                        (rv1, throwable1) -> invokeNextWithContext(finalCommand, rv1, throwable1));
                  return;
               }
            } catch (Throwable t) {
               throwable = t;
               if (t instanceof CompletionException) {
                  throwable = t.getCause();
               }
               if (trace)
                  log.tracef("Interceptor %s threw exception %s", className(interceptor), throwable);
               action = INVOKE_NEXT;
               // Skip the remaining interceptors
               interceptorNode = null;
               nextInterceptor = null;
            }
         } else if (nextReturnHandler != null) {
            // Interceptors are done, continue with the return handlers
            SequentialInterceptor.ReturnHandler returnHandler = nextReturnHandler.returnHandler;
            nextReturnHandler = nextReturnHandler.nextNode;
            if (trace)
               log.tracef("Executing return handler %s with return value/exception %s/%s", nextReturnHandler,
                     returnHandler, className(returnValue), throwable);
            try {
               CompletableFuture<Object> handlerFuture = returnHandler.handle(this, command, returnValue, throwable);
               if (handlerFuture != null) {
                  if (handlerFuture.isDone()) {
                     // The future is already completed.
                     // Update the return value and continue with the next return handler.
                     // If the future was a ForkInfo, we will continue with an interceptor instead.
                     returnValue = handlerFuture.getNow(returnValue);
                     throwable = null;
                     // In case a fork return handler changed it
                     interceptorNode = nextInterceptor;
                  } else {
                     // Continue the execution asynchronously
                     if (EXTRA_LOGS && trace)
                        log.tracef("Return handler %s continues asynchronously", returnHandler);
                     final VisitableCommand finalCommand1 = command;
                     handlerFuture.whenComplete(
                           (rv1, throwable1) -> invokeNextWithContext(finalCommand1, rv1, throwable1));
                     return;
                  }
               }
            } catch (Throwable t) {
               if (trace)
                  log.tracef("Return handler %s threw exception %s", className(returnHandler), t);
               // Reset the return value to avoid confusion
               returnValue = null;
               throwable = t;
               // In case this was a fork return handler and nextInterceptor got reset
               // Skip the remaining interceptors
               interceptorNode = null;
               nextInterceptor = null;
            }
         } else {
            // No more interceptors and no more return handlers. We are done!
            if (EXTRA_LOGS && trace)
               log.tracef("Command %s done with return value/exception %s/%s", command,
                     className(returnValue), throwable);
            completeFuture(future, returnValue, throwable);
            return;
         }
      }
   }

   @SuppressWarnings("unchecked")
   private CompletableFuture<Object> handleForkReturn(ForkInfo forkInfo, Object returnValue,
         Throwable throwable) throws Throwable {
      nextInterceptor = forkInfo.savedInterceptor;
      // We are abusing type erasure so that we can handle the future in invokeNext
      CompletableFuture handlerFuture = forkInfo.forkReturnHandler
            .handle(this, forkInfo.savedCommand, returnValue, throwable);
      return handlerFuture;
   }

   Object invokeSync(VisitableCommand command, InterceptorListNode firstInterceptor)
         throws Throwable {
      nextReturnHandler = null;
      action = INVOKE_NEXT;
      try {
         Object returnValue = invokeInterceptorsSync(command, firstInterceptor);
         return invokeReturnHandlersSync(command, returnValue, null);
      } catch (Throwable t) {
         // Unwrap the exception from CompletableFutures.await
         Throwable throwable = extractCompletableFutureException(t);
         return invokeReturnHandlersSync(command, null, throwable);
      }
   }

   private Object invokeInterceptorsSync(VisitableCommand command, InterceptorListNode firstInterceptorNode)
         throws Throwable {
      // We manually unroll interceptor loop to help the JIT inline the visitCommand methods when all
      // the caches use similar configurations.
      // For interceptors extending DDSequentialInterceptor, this will also inline the hottest visit method,
      // usually visitGetKeyValueCommand.
      InterceptorListNode interceptorNode = firstInterceptorNode;
      SequentialInterceptor interceptor = interceptorNode.interceptor;
      interceptorNode = beforeVisit(command, interceptorNode, interceptor);
      CompletableFuture<Void> nextVisitFuture = interceptor.visitCommand(this, command);
      if (afterVisit(command, nextVisitFuture)) {
         // Skip the rest of the interceptors
         return actionValue;
      }
      interceptor = interceptorNode.interceptor;
      interceptorNode = beforeVisit(command, interceptorNode, interceptor);
      nextVisitFuture = interceptor.visitCommand(this, command);
      if (afterVisit(command, nextVisitFuture)) {
         // Skip the rest of the interceptors
         return actionValue;
      }
      interceptor = interceptorNode.interceptor;
      interceptorNode = beforeVisit(command, interceptorNode, interceptor);
      nextVisitFuture = interceptor.visitCommand(this, command);
      if (afterVisit(command, nextVisitFuture)) {
         // Skip the rest of the interceptors
         return actionValue;
      }
      interceptor = interceptorNode.interceptor;
      interceptorNode = beforeVisit(command, interceptorNode, interceptor);
      nextVisitFuture = interceptor.visitCommand(this, command);
      if (afterVisit(command, nextVisitFuture)) {
         // Skip the rest of the interceptors
         return actionValue;
      }
      interceptor = interceptorNode.interceptor;
      interceptorNode = beforeVisit(command, interceptorNode, interceptor);
      nextVisitFuture = interceptor.visitCommand(this, command);
      if (afterVisit(command, nextVisitFuture)) {
         // Skip the rest of the interceptors
         return actionValue;
      }
      interceptor = interceptorNode.interceptor;
      interceptorNode = beforeVisit(command, interceptorNode, interceptor);
      nextVisitFuture = interceptor.visitCommand(this, command);
      if (afterVisit(command, nextVisitFuture)) {
         // Skip the rest of the interceptors
         return actionValue;
      }
      interceptor = interceptorNode.interceptor;
      interceptorNode = beforeVisit(command, interceptorNode, interceptor);
      nextVisitFuture = interceptor.visitCommand(this, command);
      if (afterVisit(command, nextVisitFuture)) {
         // Skip the rest of the interceptors
         return actionValue;
      }
      interceptor = interceptorNode.interceptor;
      interceptorNode = beforeVisit(command, interceptorNode, interceptor);
      nextVisitFuture = interceptor.visitCommand(this, command);
      if (afterVisit(command, nextVisitFuture)) {
         // Skip the rest of the interceptors
         return actionValue;
      }
      interceptor = interceptorNode.interceptor;
      interceptorNode = beforeVisit(command, interceptorNode, interceptor);
      nextVisitFuture = interceptor.visitCommand(this, command);
      if (afterVisit(command, nextVisitFuture)) {
         // Skip the rest of the interceptors
         return actionValue;
      }
      interceptor = interceptorNode.interceptor;
      interceptorNode = beforeVisit(command, interceptorNode, interceptor);
      nextVisitFuture = interceptor.visitCommand(this, command);
      if (afterVisit(command, nextVisitFuture)) {
         // Skip the rest of the interceptors
         return actionValue;
      }
      interceptor = interceptorNode.interceptor;
      interceptorNode = beforeVisit(command, interceptorNode, interceptor);
      nextVisitFuture = interceptor.visitCommand(this, command);
      if (afterVisit(command, nextVisitFuture)) {
         // Skip the rest of the interceptors
         return actionValue;
      }
      interceptor = interceptorNode.interceptor;
      interceptorNode = beforeVisit(command, interceptorNode, interceptor);
      nextVisitFuture = interceptor.visitCommand(this, command);
      if (afterVisit(command, nextVisitFuture)) {
         // Skip the rest of the interceptors
         return actionValue;
      }
      interceptor = interceptorNode.interceptor;
      interceptorNode = beforeVisit(command, interceptorNode, interceptor);
      nextVisitFuture = interceptor.visitCommand(this, command);
      if (afterVisit(command, nextVisitFuture)) {
         // Skip the rest of the interceptors
         return actionValue;
      }
      interceptor = interceptorNode.interceptor;
      interceptorNode = beforeVisit(command, interceptorNode, interceptor);
      nextVisitFuture = interceptor.visitCommand(this, command);
      if (afterVisit(command, nextVisitFuture)) {
         // Skip the rest of the interceptors
         return actionValue;
      }
      interceptor = interceptorNode.interceptor;
      interceptorNode = beforeVisit(command, interceptorNode, interceptor);
      nextVisitFuture = interceptor.visitCommand(this, command);
      if (afterVisit(command, nextVisitFuture)) {
         // Skip the rest of the interceptors
         return actionValue;
      }
      interceptor = interceptorNode.interceptor;
      interceptorNode = beforeVisit(command, interceptorNode, interceptor);
      nextVisitFuture = interceptor.visitCommand(this, command);
      if (afterVisit(command, nextVisitFuture)) {
         // Skip the rest of the interceptors
         return actionValue;
      }
      interceptor = interceptorNode.interceptor;
      interceptorNode = beforeVisit(command, interceptorNode, interceptor);
      nextVisitFuture = interceptor.visitCommand(this, command);
      if (afterVisit(command, nextVisitFuture)) {
         // Skip the rest of the interceptors
         return actionValue;
      }
      interceptor = interceptorNode.interceptor;
      interceptorNode = beforeVisit(command, interceptorNode, interceptor);
      nextVisitFuture = interceptor.visitCommand(this, command);
      if (afterVisit(command, nextVisitFuture)) {
         // Skip the rest of the interceptors
         return actionValue;
      }
      interceptor = interceptorNode.interceptor;
      interceptorNode = beforeVisit(command, interceptorNode, interceptor);
      nextVisitFuture = interceptor.visitCommand(this, command);
      if (afterVisit(command, nextVisitFuture)) {
         // Skip the rest of the interceptors
         return actionValue;
      }
      interceptor = interceptorNode.interceptor;
      interceptorNode = beforeVisit(command, interceptorNode, interceptor);
      nextVisitFuture = interceptor.visitCommand(this, command);
      if (afterVisit(command, nextVisitFuture)) {
         // Skip the rest of the interceptors
         return actionValue;
      }

      if (interceptorNode != null) {
         throw new IllegalStateException("Too many interceptors!");
      } else {
         throw new IllegalStateException("CallInterceptor must call shortCircuit");
      }
   }

   private InterceptorListNode beforeVisit(VisitableCommand command, InterceptorListNode interceptorNode,
         SequentialInterceptor interceptor) {
      interceptorNode = interceptorNode.nextNode;
      this.nextInterceptor = interceptorNode;

      if (trace)
         log.tracef("Invoking interceptor %s with command %s", className(interceptor),
               className(command));
      return interceptorNode;
   }

   private boolean afterVisit(VisitableCommand command, CompletableFuture<Void> nextVisitFuture)
         throws Throwable {
      if (nextVisitFuture != CONTINUE_INVOCATION) {
         CompletableFutures.await(nextVisitFuture);
      }
      while (action == FORK_INVOCATION) {
         invokeForkAndHandlerSync(command);
      }
      return action == SHORT_CIRCUIT;
   }

   private void invokeForkAndHandlerSync(VisitableCommand command) throws Throwable {
      action = INVOKE_NEXT;
      ForkInfo forkInfo = (ForkInfo) actionValue;
      Object forkReturnValue = null;
      Throwable throwable = null;
      try {
         forkReturnValue = forkInvocationSync(forkInfo.newCommand);
      } catch (Throwable t) {
         throwable = t;
      }
      if (trace) {
         log.tracef("Invoking fork return handler %s with return value/exception: %s/%s",
               className(forkInfo.forkReturnHandler), className(forkReturnValue), className(throwable));
      }
      CompletableFuture handlerFuture = forkInfo.forkReturnHandler
            .handle(this, command, forkReturnValue, throwable);
      CompletableFutures.await(handlerFuture);
   }

   private Object invokeReturnHandlerSync(SequentialInterceptor.ReturnHandler returnHandler,
         VisitableCommand command, Object returnValue, Throwable throwable) throws Throwable {
      if (trace)
         log.tracef("Invoking return handler %s with return value/exception: %s/%s",
               className(returnHandler), className(returnValue), className(throwable));
      CompletableFuture<Object> handlerFuture =
            returnHandler.handle(this, command, returnValue, throwable);
      if (handlerFuture != null) {
         return CompletableFutures.await(handlerFuture);
      }

      // A return value of null means we preserve the existing return value/exception
      if (throwable != null) {
         throw throwable;
      } else {
         return returnValue;
      }
   }

   private static <T, E extends Throwable> void completeFuture(CompletableFuture<T> future, T returnValue,
         E exception) {
      if (exception == null) {
         future.complete(returnValue);
      } else {
         future.completeExceptionally(exception);
      }
   }

   private static String className(Object o) {
      if (o == null)
         return "null";

      String fullName = o.getClass().getName();
      return fullName.substring(fullName.lastIndexOf('.') + 1);
   }

   private static String actionName(int action) {
      switch (action) {
         case INVOKE_NEXT:
            return "INVOKE_NEXT";
         case SHORT_CIRCUIT:
            return "SHORT_CIRCUIT";
         case STOP_INVOCATION:
            return "STOP_INVOCATION";
         case FORK_INVOCATION:
            return "FORK_INVOCATION";
         default:
            return "Unknown action " + action;
      }
   }

   @Override
   public InvocationContext clone() {
      try {
         BaseSequentialInvocationContext clone = (BaseSequentialInvocationContext) super.clone();
         return clone;
      } catch (CloneNotSupportedException e) {
         throw new CacheException("Impossible", e);
      }
   }

   private static class ForkInfo implements SequentialInterceptor.ReturnHandler {
      final VisitableCommand newCommand;
      final SequentialInterceptor.ForkReturnHandler forkReturnHandler;
      InterceptorListNode savedInterceptor;
      VisitableCommand savedCommand;

      ForkInfo(VisitableCommand newCommand, SequentialInterceptor.ForkReturnHandler forkReturnHandler) {
         this.newCommand = newCommand;
         this.forkReturnHandler = forkReturnHandler;
      }

      @Override
      public String toString() {
         return "ForkInfo{" + newCommand.getClass().getSimpleName() + "}";
      }

      @Override
      public CompletableFuture<Object> handle(InvocationContext rCtx, VisitableCommand rCommand, Object rv,
            Throwable throwable) throws Throwable {
         return ((BaseSequentialInvocationContext) rCtx).handleForkReturn(this, rv, throwable);
      }
   }

   private static class ReturnHandlerNode {
      final SequentialInterceptor.ReturnHandler returnHandler;
      final ReturnHandlerNode nextNode;

      ReturnHandlerNode(SequentialInterceptor.ReturnHandler returnHandler, ReturnHandlerNode next) {
         this.returnHandler = returnHandler;
         this.nextNode = next;
      }
   }
}
