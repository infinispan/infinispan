package org.infinispan.interceptors.impl;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import org.infinispan.interceptors.InvocationStage;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.function.TetraConsumer;
import org.infinispan.util.function.TetraFunction;
import org.infinispan.util.function.TriConsumer;
import org.infinispan.util.function.TriFunction;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Invocation stage representing a computation that already completed successfully.
 *
 * @author Dan Berindei
 * @since 9.0
 */
public class InvocationStageImpl implements InvocationStage, BiConsumer<Object, Throwable> {
   private static final Log log = LogFactory.getLog(InvocationStageImpl.class);
   private static final boolean trace = log.isTraceEnabled();

   /**
    * Either a simple synchronous return value, or an AsyncResult (potentially already completed with an exception)
    */
   private Object result;

   public static InvocationStageImpl makeSuccessful(Object returnValue) {
      return new InvocationStageImpl(returnValue);
   }

   public static InvocationStageImpl makeExceptional(Throwable throwable) {
      return new InvocationStageImpl(AsyncResult.makeExceptional(CompletableFutures.extractException(throwable)));
   }

   public static InvocationStageImpl makeSynchronous(Object returnValue, Throwable throwable) {
      if (throwable == null) {
         return makeSuccessful(returnValue);
      } else {
         return makeExceptional(throwable);
      }
   }

   public static InvocationStageImpl makeAsynchronous(CompletableFuture<?> completableFuture) {
      if (completableFuture.isDone() && !completableFuture.isCompletedExceptionally()) {
         // We can use a simple result and not worry about join() throwing an exception
         return makeSuccessful(completableFuture.join());
      }
      InvocationStageImpl stage = new InvocationStageImpl(new AsyncResult());
      completableFuture.whenComplete(stage);
      return stage;
   }

   private static Object makeResult(Object returnValue, Throwable throwable) {
      if (throwable == null) {
         return returnValue;
      } else {
         return AsyncResult.makeExceptional(throwable);
      }
   }

   private InvocationStageImpl(Object result) {
      this.result = result;
   }


   @Override
   public Object get() throws Throwable {
      if (result instanceof AsyncResult) {
         try {
            return CompletableFutures.await(((AsyncResult) result));
         } catch (ExecutionException e) {
            throw e.getCause();
         }
      } else {
         return result;
      }
   }

   @Override
   public boolean isDone() {
      return !(result instanceof AsyncResult) || ((AsyncResult) result).isDone();
   }

   @Override
   public CompletableFuture<Object> toCompletableFuture() {
      if (result instanceof AsyncResult) {
         return ((AsyncResult) result);
      } else {
         return CompletableFuture.completedFuture(result);
      }
   }


   @Override
   public InvocationStage thenApply(Function<Object, Object> function) {
      if (result instanceof AsyncResult) {
         return addHandler0(function, InvocationStageImpl::invokeThenApply0);
      } else {
         try {
            this.result = function.apply(result);
            return this;
         } catch (Throwable t) {
            this.result = AsyncResult.makeExceptional(t);
            return this;
         }
      }
   }

   private static Object invokeThenApply0(Object callback, Object rv, Throwable throwable) {
      Function<Object, InvocationStage> function = castCallback(callback);
      if (throwable != null) {
         return rv;
      }

      return function.apply(rv);
   }

   @Override
   public InvocationStage thenAccept(Consumer<Object> action) {
      if (result instanceof AsyncResult) {
         return addHandler0(action, InvocationStageImpl::invokeThenAccept0);
      } else {
         try {
            action.accept(result);
            return this;
         } catch (Throwable t) {
            this.result = AsyncResult.makeExceptional(t);
            return this;
         }
      }
   }

   private static Object invokeThenAccept0(Object callback, Object rv, Throwable throwable) {
      Consumer<Object> action = castCallback(callback);
      if (throwable != null) {
         return AsyncResult.makeExceptional(throwable);
      }

      action.accept(rv);
      return rv;
   }

   @Override
   public InvocationStage thenCompose(Function<Object, InvocationStage> function) {
      if (result instanceof AsyncResult) {
         return addHandler0(function, InvocationStageImpl::invokeThenCompose0);
      } else {
         try {
            InvocationStage stage = function.apply(result);
            this.result = extractResult(stage);
            return this;
         } catch (Throwable t) {
            this.result = AsyncResult.makeExceptional(t);
            return this;
         }
      }
   }

   private static Object invokeThenCompose0(Object callback, Object rv, Throwable throwable) {
      Function<Object, InvocationStage> function = castCallback(callback);
      if (throwable != null) {
         return rv;
      }

      InvocationStage stage = function.apply(rv);
      return extractResult(stage);
   }

   @Override
   public InvocationStage thenCombine(CompletionStage<?> otherStage, BiFunction<Object, Object, Object> function) {
      if (result instanceof AsyncResult) {
         InvocationStage otherInvocationStage = makeAsynchronous(otherStage.toCompletableFuture());
         return thenCompose(otherInvocationStage, function, (rStage, rFunction, rv) -> rStage.thenApply(rv, rFunction));
      } else {
         try {
            Object rv1 = result;
            this.result = makeAsynchronous(otherStage.thenApply(rv2 -> function.apply(rv1, rv2)).toCompletableFuture());
            return this;
         } catch (Throwable t) {
            this.result = AsyncResult.makeExceptional(t);
            return this;
         }
      }
   }

   @Override
   public InvocationStage thenCombine(InvocationStage otherStage, BiFunction<Object, Object, Object> function) {
      if (result instanceof AsyncResult) {
         return thenCompose(otherStage, function, (rStage, rFunction, rv) -> rStage.thenApply(rv, rFunction));
      } else {
         try {
            Object rv1 = this.result;
            this.result = otherStage.thenApply(function, rv1, BiFunction::apply);
            return this;
         } catch (Throwable t) {
            this.result = AsyncResult.makeExceptional(t);
            return this;
         }
      }
   }

   @Override
   public InvocationStage exceptionally(Function<Throwable, Object> function) {
      if (result instanceof AsyncResult) {
         return addHandler0(function, InvocationStageImpl::invokeExceptionally0);
      } else {
         // Exceptional stages always have an AsyncResult
         return this;
      }
   }

   private static Object invokeExceptionally0(Object callback, Object rv, Throwable throwable) {
      Function<Object, InvocationStage> function = castCallback(callback);
      if (throwable == null) {
         return rv;
      }

      return function.apply(throwable);
   }

   @Override
   public InvocationStage exceptionallyCompose(Function<Throwable, InvocationStage> function) {
      if (result instanceof AsyncResult) {
         return addHandler0(function, InvocationStageImpl::invokeExceptionallyCompose0);
      } else {
         // Exceptional stages always have an AsyncResult
         return this;
      }
   }

   private static Object invokeExceptionallyCompose0(Object callback, Object rv, Throwable throwable) {
      Function<Object, InvocationStage> function = castCallback(callback);
      if (throwable == null) {
         return rv;
      }

      InvocationStage stage = function.apply(throwable);
      return extractResult(stage);
   }

   @Override
   public InvocationStage handle(BiFunction<Object, Throwable, Object> function) {
      if (result instanceof AsyncResult) {
         return addHandler0(function, InvocationStageImpl::invokeHandle0);
      } else {
         try {
            this.result = function.apply(result, null);
            return this;
         } catch (Throwable t) {
            this.result = AsyncResult.makeExceptional(t);
            return this;
         }
      }
   }

   private static Object invokeHandle0(Object callback, Object rv, Throwable throwable) {
      BiFunction<Object, Throwable, Object> function = castCallback(callback);
      return function.apply(rv, throwable);
   }

   @Override
   public InvocationStage whenComplete(BiConsumer<Object, Throwable> action) {
      if (result instanceof AsyncResult) {
         return addHandler0(action, InvocationStageImpl::invokeWhenComplete0);
      } else {
         try {
            action.accept(result, null);
            return this;
         } catch (Throwable t) {
            this.result = AsyncResult.makeExceptional(t);
            return this;
         }
      }
   }

   private static Object invokeWhenComplete0(Object callback, Object rv, Throwable throwable) {
      BiConsumer<Object, Throwable> action = castCallback(callback);
      action.accept(rv, throwable);
      return makeResult(rv, throwable);
   }

   @Override
   public InvocationStage compose(BiFunction<Object, Throwable, InvocationStage> function) {
      if (result instanceof AsyncResult) {
         return addHandler0(function, InvocationStageImpl::invokeCompose0);
      } else {
         try {
            this.result = extractResult(function.apply(result, null));
            return this;
         } catch (Throwable t) {
            this.result = AsyncResult.makeExceptional(t);
            return this;
         }
      }
   }

   private static Object invokeCompose0(Object callback, Object rv, Throwable throwable) {
      BiFunction<Object, Throwable, InvocationStage> function = castCallback(callback);
      InvocationStage stage = function.apply(rv, throwable);
      return extractResult(stage);
   }

   @Override
   public <P1> InvocationStage thenApply(P1 p1, BiFunction<P1, Object, Object> function) {
      if (result instanceof AsyncResult) {
         return addHandler1(function, p1, InvocationStageImpl::invokeThenApply1);
      } else {
         try {
            this.result = function.apply(p1, result);
            return this;
         } catch (Throwable t) {
            this.result = AsyncResult.makeExceptional(t);
            return this;
         }
      }
   }

   private static Object invokeThenApply1(Object callback, Object p1, Object rv, Throwable throwable) {
      BiFunction<Object, Object, InvocationStage> function = castCallback(callback);
      if (throwable != null) {
         return rv;
      }

      return function.apply(p1, rv);
   }

   @Override
   public <P1> InvocationStage thenAccept(P1 p1, BiConsumer<P1, Object> action) {
      if (result instanceof AsyncResult) {
         return addHandler1(action, p1, InvocationStageImpl::invokeThenAccept1);
      } else {
         try {
            action.accept(p1, result);
            return this;
         } catch (Throwable t) {
            this.result = AsyncResult.makeExceptional(t);
            return this;
         }
      }
   }

   private static Object invokeThenAccept1(Object callback, Object p1, Object rv, Throwable throwable) {
      BiConsumer<Object, Object> action = castCallback(callback);
      if (throwable != null) {
         return AsyncResult.makeExceptional(throwable);
      }

      action.accept(p1, rv);
      return rv;
   }

   @Override
   public <P1> InvocationStage thenCompose(P1 p1, BiFunction<P1, Object, InvocationStage> function) {
      if (result instanceof AsyncResult) {
         return addHandler1(function, p1, InvocationStageImpl::invokeThenCompose1);
      } else {
         try {
            InvocationStage stage = function.apply(p1, result);
            this.result = extractResult(stage);
            return this;
         } catch (Throwable t) {
            this.result = AsyncResult.makeExceptional(t);
            return this;
         }
      }
   }

   private static Object invokeThenCompose1(Object callback, Object p1, Object rv, Throwable throwable) {
      BiFunction<Object, Object, InvocationStage> function = castCallback(callback);
      if (throwable != null) {
         return rv;
      }

      InvocationStage stage = function.apply(p1, rv);
      return extractResult(stage);
   }

   @Override
   public <P1> InvocationStage exceptionally(P1 p1, BiFunction<P1, Throwable, Object> function) {
      if (result instanceof AsyncResult) {
         return addHandler1(function, p1, InvocationStageImpl::invokeExceptionally1);
      } else {
         // Exceptional stages always have an AsyncResult
         return this;
      }
   }

   private static Object invokeExceptionally1(Object callback, Object p1, Object rv, Throwable throwable) {
      BiFunction<Object, Object, InvocationStage> function = castCallback(callback);
      if (throwable == null) {
         return rv;
      }

      return function.apply(p1, throwable);
   }

   @Override
   public <P1> InvocationStage exceptionallyCompose(P1 p1, BiFunction<P1, Throwable, InvocationStage> function) {
      if (result instanceof AsyncResult) {
         return addHandler1(function, p1, InvocationStageImpl::invokeExceptionallyCompose1);
      } else {
         // Exceptional stages always have an AsyncResult
         return this;
      }
   }

   private static Object invokeExceptionallyCompose1(Object callback, Object p1, Object rv, Throwable throwable) {
      BiFunction<Object, Object, InvocationStage> function = castCallback(callback);
      if (throwable == null) {
         return rv;
      }

      InvocationStage stage = function.apply(p1, throwable);
      return extractResult(stage);
   }

   @Override
   public <P1> InvocationStage handle(P1 p1, TriFunction<P1, Object, Throwable, Object> function) {
      if (result instanceof AsyncResult) {
         return addHandler1(function, p1, InvocationStageImpl::invokeHandle1);
      } else {
         try {
            this.result = function.apply(p1, result, null);
            return this;
         } catch (Throwable t) {
            this.result = AsyncResult.makeExceptional(t);
            return this;
         }
      }
   }

   private static Object invokeHandle1(Object callback, Object p1, Object rv, Throwable throwable) {
      TriFunction<Object, Object, Throwable, Object> function = castCallback(callback);
      return function.apply(p1, rv, throwable);
   }

   @Override
   public <P1> InvocationStage whenComplete(P1 p1, TriConsumer<P1, Object, Throwable> action) {
      if (result instanceof AsyncResult) {
         return addHandler1(action, p1, InvocationStageImpl::invokeWhenComplete1);
      } else {
         try {
            action.accept(p1, result, null);
            return this;
         } catch (Throwable t) {
            this.result = AsyncResult.makeExceptional(t);
            return this;
         }
      }
   }

   private static Object invokeWhenComplete1(Object callback, Object p1, Object rv, Throwable throwable) {
      TriConsumer<Object, Object, Throwable> action = castCallback(callback);
      action.accept(p1, rv, throwable);
      return makeResult(rv, throwable);
   }

   @Override
   public <P1> InvocationStage compose(P1 p1, TriFunction<P1, Object, Throwable, InvocationStage> function) {
      if (result instanceof AsyncResult) {
         return addHandler1(function, p1, InvocationStageImpl::invokeCompose1);
      } else {
         try {
            this.result = extractResult(function.apply(p1, result, null));
            return this;
         } catch (Throwable t) {
            this.result = AsyncResult.makeExceptional(t);
            return this;
         }
      }
   }

   private static Object invokeCompose1(Object callback, Object p1, Object rv, Throwable throwable) {
      TriFunction<Object, Object, Throwable, InvocationStage> function = castCallback(callback);
      InvocationStage stage = function.apply(p1, rv, throwable);
      return extractResult(stage);
   }

   @Override
   public <P1, P2> InvocationStage thenApply(P1 p1, P2 p2, TriFunction<P1, P2, Object, Object> function) {
      if (result instanceof AsyncResult) {
         return addHandler2(function, p1, p2, InvocationStageImpl::invokeThenApply2);
      } else {
         try {
            this.result = function.apply(p1, p2, result);
            return this;
         } catch (Throwable t) {
            this.result = AsyncResult.makeExceptional(t);
            return this;
         }
      }
   }

   private static Object invokeThenApply2(Object callback, Object p1, Object p2, Object rv, Throwable throwable) {
      TriFunction<Object, Object, Object, InvocationStage> function = castCallback(callback);
      if (throwable != null) {
         return rv;
      }

      return function.apply(p1, p2, rv);
   }

   @Override
   public <P1, P2> InvocationStage thenAccept(P1 p1, P2 p2, TriConsumer<P1, P2, Object> action) {
      if (result instanceof AsyncResult) {
         return addHandler2(action, p1, p2, InvocationStageImpl::invokeThenAccept2);
      } else {
         try {
            action.accept(p1, p2, result);
            return this;
         } catch (Throwable t) {
            this.result = AsyncResult.makeExceptional(t);
            return this;
         }
      }
   }

   private static Object invokeThenAccept2(Object callback, Object p1, Object p2, Object rv, Throwable throwable) {
      TriConsumer<Object, Object, Object> action = castCallback(callback);
      if (throwable != null) {
         return AsyncResult.makeExceptional(throwable);
      }

      action.accept(p1, p2, rv);
      return rv;
   }

   @Override
   public <P1, P2> InvocationStage thenCompose(P1 p1, P2 p2, TriFunction<P1, P2, Object, InvocationStage> function) {
      if (result instanceof AsyncResult) {
         return addHandler2(function, p1, p2, InvocationStageImpl::invokeThenCompose2);
      } else {
         try {
            InvocationStage stage = function.apply(p1, p2, result);
            this.result = extractResult(stage);
            return this;
         } catch (Throwable t) {
            this.result = AsyncResult.makeExceptional(t);
            return this;
         }
      }
   }

   private static Object invokeThenCompose2(Object callback, Object p1, Object p2, Object rv, Throwable throwable) {
      TriFunction<Object, Object, Object, InvocationStage> function = castCallback(callback);
      if (throwable != null) {
         return rv;
      }

      InvocationStage stage = function.apply(p1, p2, rv);
      return extractResult(stage);
   }

   @Override
   public <P1, P2> InvocationStage exceptionally(P1 p1, P2 p2, TriFunction<P1, P2, Throwable, Object> function) {
      if (result instanceof AsyncResult) {
         return addHandler2(function, p1, p2, InvocationStageImpl::invokeExceptionally2);
      } else {
         // Exceptional stages always have an AsyncResult
         return this;
      }
   }

   private static Object invokeExceptionally2(Object callback, Object p1, Object p2, Object rv, Throwable throwable) {
      TriFunction<Object, Object, Object, InvocationStage> function = castCallback(callback);
      if (throwable == null) {
         return rv;
      }

      return function.apply(p1, p2, throwable);
   }

   @Override
   public <P1, P2> InvocationStage exceptionallyCompose(P1 p1, P2 p2,
                                                        TriFunction<P1, P2, Throwable, InvocationStage> function) {
      if (result instanceof AsyncResult) {
         return addHandler2(function, p1, p2, InvocationStageImpl::invokeExceptionallyCompose2);
      } else {
         // Exceptional stages always have an AsyncResult
         return this;
      }
   }

   private static Object invokeExceptionallyCompose2(Object callback, Object p1, Object p2, Object rv,
                                                     Throwable throwable) {
      TriFunction<Object, Object, Object, InvocationStage> function = castCallback(callback);
      if (throwable == null) {
         return rv;
      }

      InvocationStage stage = function.apply(p1, p2, throwable);
      return extractResult(stage);
   }

   @Override
   public <P1, P2> InvocationStage handle(P1 p1, P2 p2, TetraFunction<P1, P2, Object, Throwable, Object> function) {
      if (result instanceof AsyncResult) {
         return addHandler2(function, p1, p2, InvocationStageImpl::invokeHandle2);
      } else {
         try {
            this.result = function.apply(p1, p2, result, null);
            return this;
         } catch (Throwable t) {
            this.result = AsyncResult.makeExceptional(t);
            return this;
         }
      }
   }

   private static Object invokeHandle2(Object callback, Object p1, Object p2, Object rv, Throwable throwable) {
      TetraFunction<Object, Object, Object, Throwable, Object> function = castCallback(callback);
      return function.apply(p1, p2, rv, throwable);
   }

   @Override
   public <P1, P2> InvocationStage whenComplete(P1 p1, P2 p2, TetraConsumer<P1, P2, Object, Throwable> action) {
      if (result instanceof AsyncResult) {
         return addHandler2(action, p1, p2, InvocationStageImpl::invokeWhenComplete2);
      } else {
         try {
            action.accept(p1, p2, result, null);
            return this;
         } catch (Throwable t) {
            this.result = AsyncResult.makeExceptional(t);
            return this;
         }
      }
   }

   private static Object invokeWhenComplete2(Object callback, Object p1, Object p2, Object rv, Throwable throwable) {
      TetraConsumer<Object, Object, Object, Throwable> action = castCallback(callback);
      action.accept(p1, p2, rv, throwable);
      return makeResult(rv, throwable);
   }

   @Override
   public <P1, P2> InvocationStage compose(P1 p1, P2 p2,
                                           TetraFunction<P1, P2, Object, Throwable, InvocationStage> function) {
      if (result instanceof AsyncResult) {
         return addHandler2(function, p1, p2, InvocationStageImpl::invokeCompose2);
      } else {
         try {
            this.result = extractResult(function.apply(p1, p2, result, null));
            return this;
         } catch (Throwable t) {
            this.result = AsyncResult.makeExceptional(t);
            return this;
         }
      }
   }

   private static Object invokeCompose2(Object callback, Object p1, Object p2, Object rv, Throwable throwable) {
      TetraFunction<Object, Object, Object, Throwable, InvocationStage> function = castCallback(callback);
      InvocationStage stage = function.apply(p1, p2, rv, throwable);
      return extractResult(stage);
   }


   @Override
   public void accept(Object o, Throwable throwable) {
      // We started with a valueFuture, and that valueFuture is now complete.
      invokeHandlers(o, throwable != null ? CompletableFutures.extractException(throwable) : null);
   }

   @Override
   public String toString() {
      return "InvocationStageImpl(" + Stages.className(result) + ")";
   }


   private InvocationStage addHandler0(Object handler, AsyncResult.Invoker0 invoker) {
      AsyncResult asyncResult = (AsyncResult) this.result;
      if (asyncResult.dequeAdd0(invoker, handler)) {
         return this;
      }

      // Deque is frozen, it means join() won't block (too much)
      // because the stage is already completed (or will be completed soon)
      Object returnValue = null;
      Throwable throwable = null;
      try {
         returnValue = asyncResult.join();
      } catch (Throwable t) {
         throwable = t;
      }
      try {
         this.result = invoker.invoke(handler, returnValue, throwable);
         return this;
      } catch (Throwable t) {
         return makeExceptional(t);
      }
   }

   private InvocationStage addHandler1(Object handler, Object p1, AsyncResult.Invoker1 invoker) {
      AsyncResult asyncResult = (AsyncResult) this.result;
      if (asyncResult.dequeAdd1(invoker, handler, p1)) {
         return this;
      }

      // Deque is frozen, it means join() won't block (too much)
      // because the stage is already completed (or will be completed soon)
      Object returnValue = null;
      Throwable throwable = null;
      try {
         returnValue = asyncResult.join();
      } catch (Throwable t) {
         throwable = t;
      }
      try {
         this.result = invoker.invoke(handler, p1, returnValue, throwable);
         return this;
      } catch (Throwable t) {
         return makeExceptional(t);
      }
   }

   private InvocationStage addHandler2(Object handler, Object p1, Object p2, AsyncResult.Invoker2 invoker) {
      AsyncResult asyncResult = (AsyncResult) this.result;
      if (asyncResult.dequeAdd2(invoker, handler, p1, p2)) {
         return this;
      }

      // Deque is frozen, it means join() won't block (too much)
      // because the stage is already completed (or will be completed soon)
      Object returnValue = null;
      Throwable throwable = null;
      try {
         returnValue = asyncResult.join();
      } catch (Throwable t) {
         throwable = t;
      }
      try {
         this.result = invoker.invoke(handler, p1, p2, returnValue, throwable);
         return this;
      } catch (Throwable t) {
         return makeExceptional(t);
      }
   }

   private void invokeHandlers(Object returnValue, Throwable throwable) {
      AsyncResult asyncResult = (AsyncResult) this.result;
      if (trace) log.tracef("Resuming invocation with %d handlers", asyncResult.dequeSize());
      while (true) {
         AsyncResult.DequeInvoker invoker;
         invoker = asyncResult.dequePoll();

         if (invoker == null) {
            // Complete the future.
            // We finished running the handlers, and the last pollHandler() call locked the deque.
            asyncResult.complete(returnValue, throwable);
            return;
         }

         // Run the handler
         try {
            Object newResult = invokeQueuedHandler(asyncResult, invoker, returnValue, throwable);
            if ((newResult instanceof AsyncResult)) {
               AsyncResult newAsyncResult = (AsyncResult) newResult;
               AsyncResult.Invoker0 invokeMe = (callback, rv, throwable1) -> {
                  BiConsumer<Object, Throwable> action = castCallback(callback);
                  action.accept(rv, throwable1);
                  return makeResult(rv, throwable1);
               };
               if (newAsyncResult.dequeAdd0(invokeMe, this)) {
                  // We stop running more handlers and continue only that AsyncResult is done.
                  return;
               } else {
                  // Deque is frozen, it means join() won't block (too much)
                  // because the stage is already completed (or will be completed soon)
                  returnValue = newAsyncResult.join();
                  throwable = null;
               }
            } else {
               // We got a simple sync result, continue with that
               returnValue = newResult;
               throwable = null;
            }
         } catch (Throwable t) {
            throwable = CompletableFutures.extractException(t);
         }
      }
   }

   private static Object invokeQueuedHandler(AsyncResult asyncResult, AsyncResult.DequeInvoker invoker,
                                             Object returnValue, Throwable throwable) {
      if (invoker instanceof AsyncResult.Invoker0) {
         return invokeQueuedHandler0(asyncResult, (AsyncResult.Invoker0) invoker, returnValue, throwable);
      } else if (invoker instanceof AsyncResult.Invoker1) {
         return invokeQueuedHandler1(asyncResult, (AsyncResult.Invoker1) invoker, returnValue, throwable);
      } else {
         return invokeQueuedHandler2(asyncResult, (AsyncResult.Invoker2) invoker, returnValue, throwable);
      }
   }

   private static Object invokeQueuedHandler0(AsyncResult asyncResult, AsyncResult.Invoker0 invoker, Object returnValue,
                                              Throwable throwable) {
      Object handler = asyncResult.dequePoll();
      return invoker.invoke(handler, returnValue, throwable);
   }

   private static Object invokeQueuedHandler1(AsyncResult asyncResult, AsyncResult.Invoker1 invoker, Object returnValue,
                                              Throwable throwable) {
      Object handler = asyncResult.dequePoll();
      Object p1 = asyncResult.dequePoll();
      return invoker.invoke(handler, p1, returnValue, throwable);
   }

   private static Object invokeQueuedHandler2(AsyncResult asyncResult, AsyncResult.Invoker2 invoker, Object returnValue,
                                              Throwable throwable) {
      Object handler = asyncResult.dequePoll();
      Object p1 = asyncResult.dequePoll();
      Object p2 = asyncResult.dequePoll();
      return invoker.invoke(handler, p1, p2, returnValue, throwable);
   }

   private static Object extractResult(InvocationStage stage) {
      return ((InvocationStageImpl) stage).result;
   }

   @SuppressWarnings("unchecked")
   private static <T> T castCallback(Object callback) {
      return (T) callback;
   }
}
