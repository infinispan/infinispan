package org.infinispan.test;

import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.withSettings;

import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.Function;

import org.infinispan.Cache;
import org.infinispan.notifications.cachelistener.cluster.ClusterCacheNotifier;
import org.infinispan.test.fwk.CheckPoint;
import org.mockito.AdditionalAnswers;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.mockito.stubbing.Stubber;
import org.reactivestreams.Publisher;

import io.reactivex.Flowable;

/**
 * Utility methods for dealing with Mockito mocks.
 *
 * @author Dan Berindei
 * @since 9.0
 */
public class Mocks {
   private Mocks() { }
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

   /**
    * Checkpoint name that is triggered to tell the test that the code has reached a spot just before invocation. This
    * thread will not proceed with the invocation until {@link Mocks#BEFORE_RELEASE} is released.
    */
   public static final String BEFORE_INVOCATION = "pre_invoked";

   /**
    * Checkpoint name that this code waits on before the invocation until the test triggers it. This will require
    * triggering per invocation if there are more than one.
    */
   public static final String BEFORE_RELEASE = "pre_released";

   /**
    * Checkpoint name that is triggered to tell the test that the code has reached a spot just after invocation. This
    * thread will not proceed after the invocation until {@link Mocks#AFTER_RELEASE} is released.
    */
   public static final String AFTER_INVOCATION = "post_invoked";

   /**
    * Checkpoint name that this code waits on after the invocation until the test triggers it. This will require
    * triggering per invocation if there are more than one.
    */
   public static final String AFTER_RELEASE = "post_released";

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
    * @param classToMock the actual class from the component registry to mock
    * @param cache the cache to replace the object on
    * @param mockStubConsumer the consumer to invoke the method on the stubber and the actual mock
    * @param <Mock> the class of objec to replace
    * @return the original object to put back into the cache
    */
   public static <Mock> Mock blockingMock(final CheckPoint checkPoint, Class<? extends Mock> classToMock,
         Cache<?, ?> cache, BiConsumer<? super Stubber, ? super Mock> mockStubConsumer) {
      return blockingMock(checkPoint, classToMock, cache, AdditionalAnswers::delegatesTo, mockStubConsumer);
   }

   /**
    * The same as {@link Mocks#blockingMock(CheckPoint, Class, Cache, BiConsumer)} except the user can also modify
    * how the default answers are produced.
    * @param checkPoint the check point to use to control blocking
    * @param classToMock the actual class from the component registry to mock
    * @param cache the cache to replace the object on
    * @param answerFunction the function to map the real object to an answer
    * @param mockStubConsumer the consumer to invoke the method on the stubber and the actual mock
    * @param <Mock> the class of objec to replace
    * @return the original object to put back into the cache
    */
   public static <Mock> Mock blockingMock(final CheckPoint checkPoint, Class<? extends Mock> classToMock,
         Cache<?, ?> cache, Function<? super Mock, Answer> answerFunction, BiConsumer<? super Stubber, ? super Mock> mockStubConsumer) {
      Mock realObject = TestingUtil.extractComponent(cache, classToMock);
      Answer forwardedAnswer = answerFunction.apply(realObject);
      Mock mock = mock(classToMock, withSettings().extraInterfaces(ClusterCacheNotifier.class).defaultAnswer(forwardedAnswer));
      mockStubConsumer.accept(doAnswer(blockingAnswer(forwardedAnswer, checkPoint)), mock);
      TestingUtil.replaceComponent(cache, classToMock, mock, true);
      return realObject;
   }

   /**
    * Spies on the given object, but allows the caller to block on any method defined in the <b>mockStubConsumer</b>.
    * Please see {@link #blockingMock(CheckPoint, Class, Cache, BiConsumer)} for more information on how the blocking
    * works.
    * @param checkPoint the check point to use to control blocking
    * @param objectToSpy the spied object
    * @param mockStubConsumer the consumer to invoke the method on the stubber and the actual mock
    * @param <Mock> type of the spied object
    * @return the new spy mock that will delegate all calls to the original object
    */
   public static <Mock> Mock blockingSpy(final CheckPoint checkPoint, Mock objectToSpy,
         BiConsumer<? super Stubber, ? super Mock> mockStubConsumer) {
      Mock mock = spy(objectToSpy);
      mockStubConsumer.accept(doAnswer(blockingAnswer(AdditionalAnswers.delegatesTo(objectToSpy), checkPoint)), mock);
      return mock;
   }

   /**
    * Allows for decorating an existing answer to apply before and after invocation and release checkpoints as
    * described in {@link Mocks#blockingMock(CheckPoint, Class, Cache, BiConsumer)}.
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
    * is completed. Uses the same checkpoint names as {@link Mocks#blockingMock(CheckPoint, Class, Cache, BiConsumer)}.
    * Note this method returns another Callable as we may not want to always block the invoking thread.
    * @param completableFutureCallable callable to invoke between blocking
    * @param checkPoint the checkpoint to use
    * @param <V> the answer from the future
    * @return a callable that will block
    */
   public static <V> Callable<CompletableFuture<V>> blockingCompletableFuture(Callable<CompletableFuture<V>> completableFutureCallable,
         CheckPoint checkPoint) {
      return () -> {
         checkPoint.trigger(BEFORE_INVOCATION);
         try {
            checkPoint.awaitStrict(BEFORE_RELEASE, 20, TimeUnit.SECONDS);
         } catch (InterruptedException e) {
            throw new AssertionError(e);
         }
         CompletableFuture<V> completableFuture = completableFutureCallable.call();
         return completableFuture.whenComplete((v, t) -> {
            checkPoint.trigger(AFTER_INVOCATION);
            try {
               checkPoint.awaitStrict(AFTER_RELEASE, 20, TimeUnit.SECONDS);
            } catch (InterruptedException | TimeoutException e) {
               throw new AssertionError(e);
            }
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
}
