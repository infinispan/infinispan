package org.infinispan.test;

import static org.infinispan.test.TestingUtil.extractGlobalComponent;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.withSettings;

import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Predicate;

import org.infinispan.Cache;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commons.util.ByRef;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.reactive.publisher.impl.Notifications;
import org.infinispan.reactive.publisher.impl.SegmentCompletionPublisher;
import org.infinispan.remoting.inboundhandler.AbstractDelegatingHandler;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.inboundhandler.Reply;
import org.infinispan.test.fwk.CheckPoint;
import org.infinispan.util.concurrent.CompletableFutures;
import org.mockito.AdditionalAnswers;
import org.mockito.MockSettings;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.mockito.stubbing.Stubber;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import io.reactivex.rxjava3.core.Flowable;

/**
 * Utility methods for dealing with Mockito mocks.
 *
 * @author Dan Berindei
 * @since 9.0
 */
public class Mocks {
   private Mocks() { }
   /**
    * Checkpoint name that is triggered to tell the test that the code has reached a spot just before invocation. This
    * thread will not proceed with the invocation until {@link Mocks#BEFORE_RELEASE} is released.
    */
   public static final String BEFORE_INVOCATION = "before_invocation";

   /**
    * Checkpoint name that this code waits on before the invocation until the test triggers it. This will require
    * triggering per invocation if there are more than one.
    */
   public static final String BEFORE_RELEASE = "before_release";

   /**
    * Checkpoint name that is triggered to tell the test that the code has reached a spot just after invocation. This
    * thread will not proceed after the invocation until {@link Mocks#AFTER_RELEASE} is released.
    */
   public static final String AFTER_INVOCATION = "after_invocation";

   /**
    * Checkpoint name that this code waits on after the invocation until the test triggers it. This will require
    * triggering per invocation if there are more than one.
    */
   public static final String AFTER_RELEASE = "after_release";

   public static final Answer<Void> EXECUTOR_RUN_ANSWER = invocation -> {
      Runnable runnable = invocation.getArgument(0);
      runnable.run();
      return null;
   };

   public static Answer<Void> justRunExecutorAnswer() {
      return EXECUTOR_RUN_ANSWER;
   }

   public static Answer<Void> runWithExecutorAnswer(Executor executor) {
      return invocation -> {
         Runnable runnable = invocation.getArgument(0);
         executor.execute(runnable);
         return null;
      };
   }

   /**
    * Delegates a Mockito invocation to a target object, and returns the mock instead of the target object.
    *
    * Useful when {@code Mockito.spy(object)} doesn't work and the mocked class has a fluent interface.
    */
   public static <T, R> R invokeAndReturnMock(InvocationOnMock i, T target)
         throws IllegalAccessException, InvocationTargetException {
      Object returnValue = i.getMethod().invoke(target, i.getArguments());
      // If necessary, replace the return value with the mock
      return (returnValue == target) ? (R) i.getMock() : (R) returnValue;
   }

   public static <T> T callRealMethod(InvocationOnMock invocation) {
      try {
         return (T) invocation.callRealMethod();
      } catch (Throwable throwable) {
         throw CompletableFutures.asCompletionException(throwable);
      }
   }

   public static <T> T callAnotherAnswer(Answer<?> answer, InvocationOnMock invocation) {
      try {
         return (T) answer.answer(invocation);
      } catch (Throwable throwable) {
         throw CompletableFutures.asCompletionException(throwable);
      }
   }

   /**
    * Helper that provides the ability to replace a component from the cache and automatically mocks around it, returning
    * the same results as the original object. The provided stub will provide blocking when the mock uses this
    * <pre>
    *    {@code (stubber, mock) -> stubber.when(mock).methodToBlock(); }
    * </pre>The caller can then
    * control how the blocking occurs as the mock will do the following
    *
    * <pre> {@code
    * checkpoint.trigger(BEFORE_INVOCATION);
    * checkpoint.await(BEFORE_RELEASE);
    *  ... do actual invocation ...
    * checkpoint.trigger(AFTER_INVOCATION);
    * checkpoint.await(AFTER_RELEASE);
    * return result;
    * }
    * </pre>
    *
    * The user must release the BEFORE_RELEASE and AFTER_RELEASE checkpoints or else these will timeout and cause
    * test instabilities.
    * @param checkPoint the check point to use to control blocking
    * @param componentClass the actual class from the component registry to mock
    * @param cache the cache to replace the object on
    * @param mockStubConsumer the consumer to invoke the method on the stubber and the actual mock
    * @param <Mock> the class of objec to replace
    * @return the original object to put back into the cache
    */
   public static <Mock> Mock blockingMock(final CheckPoint checkPoint, Class<? extends Mock> componentClass,
                                          Cache<?, ?> cache, BiConsumer<? super Stubber, ? super Mock> mockStubConsumer,
                                          Class<?>... extraInterfaces) {
      return interceptComponent(componentClass, cache, (realObject, mock) -> {
         mockStubConsumer.accept(doAnswer(blockingAnswer(AdditionalAnswers.delegatesTo(realObject), checkPoint)), mock);
      }, extraInterfaces);
   }

   public static <Mock> Mock interceptComponent(Class<? extends Mock> componentClass, Cache<?, ?> cache,
                                                 BiConsumer<? super Mock, ? super Mock> methodInterceptor,
                                                 Class<?>... extraInterfaces) {
      Mock realObject = TestingUtil.extractComponent(cache, componentClass);
      Answer<?> forwardingAnswer = AdditionalAnswers.delegatesTo(realObject);
      MockSettings mockSettings = withSettings().defaultAnswer(forwardingAnswer);
      if (extraInterfaces != null && extraInterfaces.length > 0) {
         mockSettings.extraInterfaces(extraInterfaces);
      }
      Mock mock = mock(componentClass, mockSettings);
      methodInterceptor.accept(realObject, mock);
      TestingUtil.replaceComponent(cache, componentClass, mock, true);
      return realObject;
   }


   /**
    * Allows for decorating an existing answer to apply before and after invocation and release checkpoints as
    * described in {@link Mocks#blockingMock(CheckPoint, Class, Cache, BiConsumer, Class[])}.
    * @param answer the answer to decorate with a blocking one
    * @param checkPoint the checkpoint to register with
    * @param <T> the result type of the answer
    * @return the new blocking answer
    */
   public static <T> Answer<T> blockingAnswer(Answer<T> answer, CheckPoint checkPoint) {
      return invocation -> {
         checkPoint.trigger(BEFORE_INVOCATION);
         checkPoint.awaitStrict(BEFORE_RELEASE, 20, TimeUnit.SECONDS);
         try {
            return answer.answer(invocation);
         } finally {
            checkPoint.trigger(AFTER_INVOCATION);
            checkPoint.awaitStrict(AFTER_RELEASE, 20, TimeUnit.SECONDS);
         }
      };
   }

   /**
    * Blocks before creation of the completable future and then subsequently blocks after the completable future
    * is completed. Uses the same checkpoint names as {@link Mocks#blockingMock(CheckPoint, Class, Cache, BiConsumer, Class[])}.
    * Note this method returns another Callable as we may not want to always block the invoking thread.
    * @param completableFutureCallable callable to invoke between blocking
    * @param checkPoint the checkpoint to use
    * @param executor the executor to run the after the stage has completed - this allows the caller to control which
    *                 thread this is ran on - as it is nondeterministic since the stage may or not be complete
    *                 when applying chained methods to it
    * @param <V> the answer from the future
    * @return a callable that will block
    */
   public static <V> Callable<CompletableFuture<V>> blockingCompletableFuture(Callable<CompletableFuture<V>> completableFutureCallable,
         CheckPoint checkPoint, Executor executor) {
      return () -> {
         checkPoint.trigger(BEFORE_INVOCATION);
         try {
            checkPoint.awaitStrict(BEFORE_RELEASE, 20, TimeUnit.SECONDS);
         } catch (InterruptedException e) {
            throw new AssertionError(e);
         }
         CompletableFuture<V> completableFuture = completableFutureCallable.call();
         return completableFuture.thenCompose(v -> {
            checkPoint.trigger(AFTER_INVOCATION);
            return checkPoint.future(AFTER_RELEASE, 20, TimeUnit.SECONDS, executor)
                  .thenApply(ignore -> v);
         });
      };
   }

   public static <E> Publisher<E> blockingPublisher(Publisher<E> publisher, CheckPoint checkPoint) {
      return Flowable.fromPublisher(publisher)
            .doOnSubscribe(s -> {
               checkPoint.trigger(BEFORE_INVOCATION);
               checkPoint.awaitStrict(BEFORE_RELEASE, 20, TimeUnit.SECONDS);
            })
            .doOnComplete(() -> {
               checkPoint.trigger(AFTER_INVOCATION);
               checkPoint.awaitStrict(AFTER_RELEASE, 20, TimeUnit.SECONDS);
            });
   }

   public static <E> SegmentCompletionPublisher<E> blockingPublisher(SegmentCompletionPublisher<E> publisher, CheckPoint checkPoint) {
      return new SegmentCompletionPublisher<E>() {
         @Override
         public void subscribeWithSegments(Subscriber<? super Notification<E>> subscriber) {
            Flowable.<Notification<E>>fromPublisher(publisher::subscribeWithSegments)
                  .doOnSubscribe(subscription -> {
                     checkPoint.trigger(BEFORE_INVOCATION);
                     checkPoint.awaitStrict(BEFORE_RELEASE, 20, TimeUnit.SECONDS);
                  })
                  .doOnComplete(() -> {
                     checkPoint.trigger(AFTER_INVOCATION);
                     checkPoint.awaitStrict(AFTER_RELEASE, 20, TimeUnit.SECONDS);
                  }).subscribe(subscriber);
         }

         @Override
         public void subscribe(Subscriber<? super E> subscriber) {
            Flowable.fromPublisher(publisher)
                  .doOnSubscribe(subscription -> {
                     checkPoint.trigger(BEFORE_INVOCATION);
                     checkPoint.awaitStrict(BEFORE_RELEASE, 20, TimeUnit.SECONDS);
                  })
                  .doOnComplete(() -> {
                     checkPoint.trigger(AFTER_INVOCATION);
                     checkPoint.awaitStrict(AFTER_RELEASE, 20, TimeUnit.SECONDS);
                  }).subscribe(subscriber);
         }
      };
   }

   /**
    * Returns a publisher that will block just before sending an element that matches the given predicate and then
    * subsequently unblocks after it finds the next element or is completed. Uses the same checkpoint names as
    * {@link Mocks#blockingMock(CheckPoint, Class, Cache, BiConsumer, Class[])}. This method can work with multiple
    * entries that pass the predicate but there is no way to distinguish which is which.
    *
    * @param publisher the publisher to use as the upstream source
    * @param checkPoint the checkpoint to block on with the mock names
    * @param predicate the predicate to test
    * @param <E> the type of the values
    * @return Publisher that will block the checkpoint while processing the elements that pass the predicate
    */
   public static <E> Publisher<E> blockingPublisherOnElement(Publisher<E> publisher, CheckPoint checkPoint,
         Predicate<? super E> predicate) {
      return Flowable.defer(() -> {
         ByRef.Boolean byRef = new ByRef.Boolean(false);
         return Flowable.fromPublisher(publisher)
               .doOnNext(e -> {
                  if (byRef.get()) {
                     byRef.set(false);
                     checkPoint.trigger(AFTER_INVOCATION);
                     checkPoint.awaitStrict(AFTER_RELEASE, 20, TimeUnit.SECONDS);
                  }
                  if (predicate.test(e)) {
                     byRef.set(true);
                     checkPoint.trigger(BEFORE_INVOCATION);
                     checkPoint.awaitStrict(BEFORE_RELEASE, 20, TimeUnit.SECONDS);
                  }
               }).doFinally(() -> {
                  if (byRef.get()) {
                     checkPoint.trigger(AFTER_INVOCATION);
                     checkPoint.awaitStrict(AFTER_RELEASE, 20, TimeUnit.SECONDS);
                  }
               });
      });
   }

   public static <E> SegmentCompletionPublisher<E> blockingSegmentPublisherOnElement(SegmentCompletionPublisher<E> publisher,
         CheckPoint checkPoint, Predicate<? super SegmentCompletionPublisher.Notification<E>> predicate) {
      return new SegmentCompletionPublisher<E>() {
         @Override
         public void subscribeWithSegments(Subscriber<? super Notification<E>> subscriber) {
            ByRef.Boolean byRef = new ByRef.Boolean(false);
            Flowable.<Notification<E>>fromPublisher(publisher::subscribeWithSegments)
                  .doOnNext(e -> {
                     if (byRef.get()) {
                        byRef.set(false);
                        checkPoint.trigger(AFTER_INVOCATION);
                        checkPoint.awaitStrict(AFTER_RELEASE, 20, TimeUnit.SECONDS);
                     }
                     if (predicate.test(e)) {
                        byRef.set(true);
                        checkPoint.trigger(BEFORE_INVOCATION);
                        checkPoint.awaitStrict(BEFORE_RELEASE, 20, TimeUnit.SECONDS);
                     }
                  }).doFinally(() -> {
                     if (byRef.get()) {
                        checkPoint.trigger(AFTER_INVOCATION);
                        checkPoint.awaitStrict(AFTER_RELEASE, 20, TimeUnit.SECONDS);
                     }
                  }).subscribe(subscriber);
         }

         @Override
         public void subscribe(Subscriber<? super E> s) {
            blockingPublisherOnElement((Publisher<E>) publisher, checkPoint,
                  value -> predicate.test(Notifications.<E>value(value)))
                  .subscribe(s);
         }
      };
   }

   public static AbstractDelegatingHandler blockInboundCacheRpcCommand(Cache<?, ?> cache, CheckPoint checkPoint,
         Predicate<? super CacheRpcCommand> predicate) {
      Executor executor = extractGlobalComponent(cache.getCacheManager(), ExecutorService.class,
            KnownComponentNames.NON_BLOCKING_EXECUTOR);
      return TestingUtil.wrapInboundInvocationHandler(cache, handler -> new AbstractDelegatingHandler(handler) {
         @Override
         public void handle(CacheRpcCommand command, Reply reply, DeliverOrder order) {
            if (!predicate.test(command)) {
               delegate.handle(command, reply, order);
               return;
            }

            checkPoint.trigger(BEFORE_INVOCATION);
            checkPoint.future(BEFORE_RELEASE, 20, TimeUnit.SECONDS, executor)
                      .thenRun(() -> delegate.handle(command, reply, order))
                      .thenCompose(ignored -> {
                         checkPoint.trigger(AFTER_INVOCATION);
                         return checkPoint.future(AFTER_RELEASE, 20, TimeUnit.SECONDS, executor);
                      });
         }
      });
   }

   /**
    * Replaces the given component with a spy and returns it for further mocking as needed. Note the original component
    * is not retrieved and thus requires retrieving before invoking this method if needed.
    *
    * @param cache          the cache to get the component from
    * @param componentClass the class of the component to retrieve
    * @param <C>            the component class
    * @return the spied component which has already been replaced and wired in the cache
    */
   public static <C> C replaceComponentWithSpy(Cache<?, ?> cache, Class<C> componentClass) {
      C component = TestingUtil.extractComponent(cache, componentClass);
      C spiedComponent = spy(component);
      TestingUtil.replaceComponent(cache, componentClass, spiedComponent, true);
      reset(spiedComponent);
      return spiedComponent;
   }
}
