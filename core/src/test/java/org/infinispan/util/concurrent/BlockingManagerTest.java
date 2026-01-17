package org.infinispan.util.concurrent;

import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;

import org.infinispan.commons.util.concurrent.CompletionStages;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.Mocks;
import org.infinispan.testing.BlockHoundHelper;
import org.mockito.Mockito;
import org.reactivestreams.Publisher;
import org.testng.annotations.Test;

import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.processors.AsyncProcessor;
import io.reactivex.rxjava3.processors.UnicastProcessor;
import io.reactivex.rxjava3.subscribers.TestSubscriber;

@Test(groups = "unit", testName = "util.concurrent.BlockingManagerTest")
public class BlockingManagerTest extends AbstractInfinispanTest {
   Executor nonBlockingExecutor;
   Executor blockingExecutor;

   public void initializeMocks() {
      nonBlockingExecutor = Mockito.mock(Executor.class, Mockito.withSettings()
            .defaultAnswer(Mocks.runWithExecutorAnswer(BlockHoundHelper.ensureNonBlockingExecutor())));
      blockingExecutor = Mockito.mock(Executor.class, Mockito.withSettings()
            .defaultAnswer(Mocks.runWithExecutorAnswer(BlockHoundHelper.allowBlockingExecutor())));
   }

   private BlockingManager createBlockingManager(boolean blockingInvocation) {
      initializeMocks();

      BlockingManagerImpl blockingManager = new BlockingManagerImpl() {
         @Override
         protected boolean isCurrentThreadBlocking() {
            return blockingInvocation;
         }
      };

      blockingManager.nonBlockingExecutor = nonBlockingExecutor;
      blockingManager.blockingExecutor = blockingExecutor;
      blockingManager.start();

      return blockingManager;
   }

   // Make sure all "results" from the Flowable are published from the non blocking executor
   private static <E> Flowable<E> ensureUserPublisherSubscribeCompleteOnBlockingThread(Publisher<E> publisher) {
         return Flowable.fromPublisher(publisher)
               .doOnSubscribe(sub -> assertFalse(BlockHoundHelper.currentThreadRequiresNonBlocking()))
               .doOnComplete(() -> assertFalse(BlockHoundHelper.currentThreadRequiresNonBlocking()));
   }

   private static <E> Flowable<E> ensurePublisherValuesOnNonBlockingThread(Publisher<E> publisher) {
      return Flowable.fromPublisher(publisher)
            .doOnNext(ignore -> assertTrue(BlockHoundHelper.currentThreadRequiresNonBlocking()))
            .doOnComplete(() -> assertTrue(BlockHoundHelper.currentThreadRequiresNonBlocking()));
   }

   public void testBlockingPublishToVoidStageInvokedBlockingThread() {
      BlockingManager blockingManager = createBlockingManager(true);

      CompletionStage<Void> stage = blockingManager.blockingPublisherToVoidStage(Flowable.fromArray(new Object[] { 1, 2, 3 })
            .doOnNext(BlockHoundHelper::blockingConsume), null);
      assertTrue(CompletionStages.isCompletedSuccessfully(stage));

      // We should not have used any executor as we were a blocking thread already
      Mockito.verifyNoInteractions(nonBlockingExecutor, blockingExecutor);
   }

   public void testBlockingPublishToVoidStageInvokedNonBlockingThread() {
      BlockingManager blockingManager = createBlockingManager(false);

      CompletionStage<Void> stage = blockingManager.blockingPublisherToVoidStage(Flowable.just(1)
            .doOnNext(BlockHoundHelper::blockingConsume), null);
      assertTrue(CompletionStages.isCompletedSuccessfully(stage));

      Mockito.verify(blockingExecutor).execute(Mockito.any());
      Mockito.verifyNoInteractions(nonBlockingExecutor);
   }

   public void testBlockingPublishToVoidStageInvokedNonBlockingThreadCompleteAfterSubscribe() {
      BlockingManager blockingManager = createBlockingManager(false);

      AsyncProcessor<Object> processor = AsyncProcessor.create();

      processor.onNext(1);

      CompletionStage<Void> stage = blockingManager.blockingPublisherToVoidStage(processor
            .doOnNext(BlockHoundHelper::blockingConsume), null);
      assertFalse(CompletionStages.isCompletedSuccessfully(stage));

      processor.onComplete();

      assertTrue(CompletionStages.isCompletedSuccessfully(stage));

      Mockito.verify(blockingExecutor).execute(Mockito.any());
      Mockito.verify(nonBlockingExecutor).execute(Mockito.any());
   }

   public void testBlockingPublisherInvokedBlockingThread() {
      BlockingManager blockingManager = createBlockingManager(true);

      Publisher<Integer> publisher = blockingManager.blockingPublisher(Flowable.just(1)
            .doOnNext(BlockHoundHelper::blockingConsume));

      TestSubscriber<Integer> subscriber = TestSubscriber.create();
      publisher.subscribe(subscriber);

      subscriber.assertComplete();

      // We should not have used any executor as we were a blocking thread already
      Mockito.verifyNoInteractions(nonBlockingExecutor, blockingExecutor);
   }

   public void testBlockingPublisherInvokedBlockingThreadCompleteAfterSubscribe() {
      BlockingManager blockingManager = createBlockingManager(true);

      AsyncProcessor<Integer> processor = AsyncProcessor.create();

      processor.onNext(1);

      Publisher<Integer> publisher = blockingManager.blockingPublisher(processor
            .doOnNext(BlockHoundHelper::blockingConsume));

      TestSubscriber<Integer> subscriber = TestSubscriber.create();
      publisher.subscribe(subscriber);

      subscriber.assertNotComplete();

      processor.onComplete();

      subscriber.assertComplete();

      // We should not have used any executor as we were a blocking thread already for onNext and onComplete is done on
      // the invoking thread as it happened after publish
      Mockito.verifyNoInteractions(nonBlockingExecutor, blockingExecutor);
   }

   public void testBlockingPublisherInvokedNonBlockingThread() {
      BlockingManager blockingManager = createBlockingManager(false);

      Publisher<Integer> publisher = blockingManager.blockingPublisher(
            ensureUserPublisherSubscribeCompleteOnBlockingThread(Flowable.just(1))
                  .doOnNext(BlockHoundHelper::blockingConsume));

      TestSubscriber<Integer> subscriber = TestSubscriber.create();
      ensurePublisherValuesOnNonBlockingThread(publisher)
            .subscribe(subscriber);

      subscriber.assertComplete();

      Mockito.verify(blockingExecutor).execute(Mockito.any());
      // This is invoked 3 times because of how AsyncProcessor works - it submits once for request, once for onNext and
      // once for onComplete
      Mockito.verify(nonBlockingExecutor, Mockito.times(3)).execute(Mockito.any());
   }

   public void testBlockingPublisherInvokedNonBlockingThreadCompleteAfterSubscribe() {
      BlockingManager blockingManager = createBlockingManager(false);

      UnicastProcessor<Integer> processor = UnicastProcessor.create();

      processor.onNext(1);

      Publisher<Integer> publisher = blockingManager.blockingPublisher(
            ensureUserPublisherSubscribeCompleteOnBlockingThread(processor)
                  .doOnNext(BlockHoundHelper::blockingConsume));

      TestSubscriber<Integer> subscriber = TestSubscriber.create();
      ensurePublisherValuesOnNonBlockingThread(publisher)
            .subscribe(subscriber);

      subscriber.assertNotComplete();

      processor.onComplete();

      subscriber.assertComplete();

      Mockito.verify(blockingExecutor).execute(Mockito.any());
      // This is invoked 3 times because of how AsyncProcessor works - it submits once for request, once for onNext and
      // once for onComplete
      Mockito.verify(nonBlockingExecutor, Mockito.times(3)).execute(Mockito.any());
   }

   public void testBlockingPublisherInvokedNonBlockingThreadCancelled() {
      BlockingManager blockingManager = createBlockingManager(false);

      int takeAmount = 5;

      Publisher<Integer> publisher = blockingManager.blockingPublisher(
            ensureUserPublisherSubscribeCompleteOnBlockingThread(Flowable.range(1, 10))
            // We need to make sure the user provided publisher is only ever subscribed or completed on the blocking
            // thread when using the blockingManager
            .doOnSubscribe(sub -> assertFalse(BlockHoundHelper.currentThreadRequiresNonBlocking()))
            .doOnComplete(() -> assertFalse(BlockHoundHelper.currentThreadRequiresNonBlocking()))
            .doOnNext(BlockHoundHelper::blockingConsume));

      TestSubscriber<Integer> subscriber = TestSubscriber.create();
      ensurePublisherValuesOnNonBlockingThread(publisher)
            .take(takeAmount)
            .subscribe(subscriber);

      subscriber.assertComplete();

      // This is invoked 1 or 2 times depending on timing
      Mockito.verify(blockingExecutor, Mockito.atMost(2)).execute(Mockito.any());
      // This is invoked 6 times because of how AsyncProcessor works - it submits once for request, and once for each onNext
      Mockito.verify(nonBlockingExecutor, Mockito.times(6)).execute(Mockito.any());
   }
}
