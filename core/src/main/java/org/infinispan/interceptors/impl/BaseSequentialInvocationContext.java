package org.infinispan.interceptors.impl;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.SequentialInvocationContext;
import org.infinispan.interceptors.SequentialInterceptor;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.jboss.logging.NDC;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

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
   private static final int ON_RETURN = 4;

   // The next interceptor to execute
   private InterceptorListNode nextInterceptor;
   // The next return handler to execute
   private ReturnHandlerNode nextReturnHandler;
   private CompletableFuture<Object> future;
   private int action;
   private Object actionValue;

   @Override
   public CompletableFuture<Void> onReturn(SequentialInterceptor.ReturnHandler returnHandler) {
      preActionCheck();
      action = ON_RETURN;
      actionValue = returnHandler;
      return CONTINUE_INVOCATION;
   }

   @Override
   public CompletableFuture<Void> continueInvocation() {
      return CONTINUE_INVOCATION;
   }

   @Override
   public CompletableFuture<Void> shortCircuit(Object returnValue) {
      preActionCheck();
      action = SHORT_CIRCUIT;
      actionValue = returnValue;
      return CONTINUE_INVOCATION;
   }

   @Override
   public CompletableFuture<Void> forkInvocation(VisitableCommand newCommand,
         SequentialInterceptor.ForkReturnHandler forkReturnHandler) {
      preActionCheck();
      InterceptorListNode localNode = this.nextInterceptor;
      if (localNode == null) {
         throw new IllegalStateException(
               "Cannot call shortCircuit or forkInvocation after all interceptors have executed");
      }
      this.action = FORK_INVOCATION;
      this.actionValue = new ForkInfo(newCommand, forkReturnHandler);
      return CONTINUE_INVOCATION;
   }

   private void preActionCheck() {
      if (action != INVOKE_NEXT) {
         throw new IllegalStateException("An interceptor can call shortCircuit, forkInvocation, or onReturn at most once. The current action is " + actionName(action));
      }
   }

   @Override
   public Object forkInvocationSync(VisitableCommand newCommand) throws Throwable {
      InterceptorListNode savedInterceptorNode = nextInterceptor;
      try {
         return doInvokeNextSync(newCommand, savedInterceptorNode);
      } finally {
         nextInterceptor = savedInterceptorNode;
      }
   }

   CompletableFuture<Object> invoke(VisitableCommand command, InterceptorListNode firstInterceptor) {
      future = new CompletableFuture<>();
      nextInterceptor = firstInterceptor;
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
            forkInfo.savedInterceptor = interceptorNode;
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
         } else if (action == ON_RETURN) {
            action = INVOKE_NEXT;
            SequentialInterceptor.ReturnHandler returnHandler =
                  (SequentialInterceptor.ReturnHandler) this.actionValue;
            nextReturnHandler = new ReturnHandlerNode(returnHandler, nextReturnHandler);
         }
         action = INVOKE_NEXT;
         if (interceptorNode != null) {
            SequentialInterceptor interceptor = interceptorNode.interceptor;
            interceptorNode = interceptorNode.nextNode;
            nextInterceptor = interceptorNode;
            if (EXTRA_LOGS && trace) {
               log.tracef("Executing interceptor %s with command %s", className(interceptor),
                     className(command));
            }
            try {
               CompletableFuture<Void> nextFuture = interceptor.visitCommand(this, command);
               if (nextFuture == null) {
                  throw new IllegalStateException(interceptor.getClass() + ".visitCommand() must not return null");
               }

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
            if (EXTRA_LOGS && trace)
               log.tracef("Executing return handler %s, returning %s/%s", nextReturnHandler, returnHandler,
                     className(returnValue), throwable);
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
               log.tracef("Command %s done, returning %s/%s", command, className(returnValue),
                     throwable);
            completeFuture(future, returnValue, throwable);
            return;
         }
      }
   }

   private CompletableFuture<Object> handleForkReturn(ForkInfo forkInfo, Object returnValue,
         Throwable throwable) throws Throwable {
      if (EXTRA_LOGS && trace) {
         log.tracef("Forked command %s done for interceptor %s, returning %s/%s", forkInfo.newCommand,
               className(forkInfo.savedInterceptor.interceptor), className(returnValue),
               throwable);
      }
      nextInterceptor = forkInfo.savedInterceptor;
      // We are abusing type erasure so that we can handle the future in invokeNext
      CompletableFuture handlerFuture = forkInfo.forkReturnHandler
            .handle(this, forkInfo.savedCommand, returnValue, throwable);
      return handlerFuture;
   }

   Object invokeSync(VisitableCommand command, InterceptorListNode firstInterceptor) throws Throwable {
      future = null;
      nextInterceptor = firstInterceptor;
      action = INVOKE_NEXT;

      return doInvokeNextSync(command, firstInterceptor);
   }

   private Object doInvokeNextSync(VisitableCommand command, InterceptorListNode interceptorNode)
         throws Throwable {
      SequentialInterceptor interceptor = interceptorNode.interceptor;
      nextInterceptor = interceptorNode.nextNode;

      // Simplify the execution for CommandInterceptors
      if (interceptor instanceof CommandInterceptor) {
         return command.acceptVisitor(this, (CommandInterceptor) interceptor);
      }

      CompletableFuture<Void> nextVisitFuture = interceptor.visitCommand(this, command);
      if (!nextVisitFuture.isDone()) {
         CompletableFutures.await(nextVisitFuture);
      }
      return this.handleActionSync(command, interceptorNode);
   }

   private Object handleActionSync(VisitableCommand command,
         InterceptorListNode interceptorNode) throws Throwable {
      if (EXTRA_LOGS && trace) {
         log.tracef("Handling action %s/%s", actionName(action), className(actionValue));
      }
      if (action == SHORT_CIRCUIT) {
         // Normally this would skip the rest of the interceptors, but here it's just a normal return
         action = INVOKE_NEXT;
         return actionValue;
      } else if (action == INVOKE_NEXT) {
         // Continue with the next interceptor
         return doInvokeNextSync(command, interceptorNode.nextNode);
      } else if (action == FORK_INVOCATION) {
         // Continue with the next interceptor, but with a new command
         action = INVOKE_NEXT;
         ForkInfo forkInfo = (ForkInfo) actionValue;
         return handleForkActionSync(forkInfo, interceptorNode.nextNode);
      } else if (action == ON_RETURN) {
         action = INVOKE_NEXT;
         return handleOnReturnActionSync(command, interceptorNode.nextNode);
      } else {
         throw new IllegalStateException("Illegal action type: " + action);
      }
   }

   private Object handleOnReturnActionSync(VisitableCommand command,
         InterceptorListNode interceptorNode) throws Throwable {
      SequentialInterceptor.ReturnHandler returnHandler =
            (SequentialInterceptor.ReturnHandler) actionValue;
      Object returnValue = null;
      Throwable throwable = null;
      try {
         returnValue = doInvokeNextSync(command, interceptorNode);
      } catch (Throwable t) {
         throwable = t;
      }
      CompletableFuture<Object> handlerFuture = returnHandler.handle(this, command, returnValue, throwable);
      if (handlerFuture != null) {
         return CompletableFutures.await(handlerFuture);
      } else {
         if (throwable != null)
            throw throwable;
         else
            return returnValue;
      }
   }

   private Object handleForkActionSync(ForkInfo forkInfo,
         InterceptorListNode interceptorNode) throws Throwable {
      Object returnValue = null;
      Throwable throwable = null;
      try {
         returnValue = forkInvocationSync(forkInfo.newCommand);
      } catch (Throwable t) {
         throwable = t;
      }
      CompletableFuture<Void> handlerFuture =
            forkInfo.forkReturnHandler.handle(this, forkInfo.savedCommand, returnValue, throwable);
      if (!handlerFuture.isDone()) {
         CompletableFutures.await(handlerFuture);
      }
      return handleActionSync(forkInfo.savedCommand, interceptorNode);
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
      return o == null ? "null" : o.getClass().getName();
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
         case ON_RETURN:
            return "ON_RETURN";
         default:
            return "Unknown action " + action;
      }
   }

   public InvocationContext clone() {
      try {
         BaseSequentialInvocationContext clone = (BaseSequentialInvocationContext) super.clone();
         return clone;
      } catch (CloneNotSupportedException e) {
         throw new CacheException("Impossible");
      }
   }

   private static class ForkInfo implements SequentialInterceptor.ReturnHandler {
      final VisitableCommand newCommand;
      final SequentialInterceptor.ForkReturnHandler forkReturnHandler;
      InterceptorListNode savedInterceptor;
      VisitableCommand savedCommand;

      public ForkInfo(VisitableCommand newCommand, SequentialInterceptor.ForkReturnHandler forkReturnHandler) {
         this.newCommand = newCommand;
         this.forkReturnHandler = forkReturnHandler;
      }

      @Override
      public String toString() {
         return "ForkInfo{" + newCommand.getClass().getSimpleName() + "}";
      }

      @Override
      public CompletableFuture<Object> handle(InvocationContext ctx, VisitableCommand command, Object rv,
            Throwable throwable) throws Throwable {
         return ((BaseSequentialInvocationContext) ctx).handleForkReturn(this, rv, throwable);
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
