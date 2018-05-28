package org.infinispan.stream.impl;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.AssertJUnit.assertEquals;

import java.util.Map;
import java.util.PrimitiveIterator;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.SmallIntSet;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.ImmortalCacheEntry;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.concurrent.WithinThreadExecutor;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.reactivestreams.Publisher;
import org.testng.annotations.Test;

import io.reactivex.Flowable;
import io.reactivex.processors.PublishProcessor;
import io.reactivex.subscribers.TestSubscriber;

/**
 * Test to verify that user listener is notified at proper time for segment completions
 * @author wburns
 * @since 9.0
 */
@Test(testName = "stream.impl.CompletionRehashPublisherDecoratorTest", groups = "functional")
public class CompletionRehashPublisherDecoratorTest {

   <S> CompletionRehashPublisherDecorator<S> createDecorator(Consumer<Supplier<PrimitiveIterator.OfInt>> userListener,
         Consumer<Object> entryConsumer) {
      return new CompletionRehashPublisherDecorator<>(AbstractCacheStream.IteratorOperation.NO_MAP, null, null, userListener,
            // Just ignore early completed segments and lost ones
            i -> {}, i -> {}, new WithinThreadExecutor(), entryConsumer, e -> ((Map.Entry) e).getKey());
   }

   <S> CompletionRehashPublisherDecorator<S> createDecorator(ConsistentHash ch, Set<Integer> segmentsForOwner,
         Set<Integer> primarySegmentsForOwner, Consumer<Supplier<PrimitiveIterator.OfInt>> internalListener,
         Consumer<Object> entryConsumer) {
      Address address = Mockito.mock(Address.class);

      if (segmentsForOwner != null) {
         when(ch.getSegmentsForOwner(eq(address))).thenReturn(segmentsForOwner);
      }
      if (primarySegmentsForOwner != null) {
         when(ch.getPrimarySegmentsForOwner(eq(address))).thenReturn(primarySegmentsForOwner);
      }

      DistributionManager dm = when(mock(DistributionManager.class).getReadConsistentHash()).thenReturn(ch).getMock();

      return new CompletionRehashPublisherDecorator<>(AbstractCacheStream.IteratorOperation.NO_MAP, dm, address,
            // Just ignore early completed segments and lost ones
            internalListener, i -> {}, i -> {}, new WithinThreadExecutor(), entryConsumer, e -> ((Map.Entry) e).getKey());
   }

   void simpleAssert(Publisher<Object> resultingPublisher, PublishProcessor<Object> valuePublisher,
         Consumer<Supplier<PrimitiveIterator.OfInt>> segmentConsumer, CompletionRehashPublisherDecorator<Object> crpd,
         Consumer<IntSet> notifySegmentsCompleted, IntSet segments) {
      // This will store the result once the stream is done
      TestSubscriber<Object> test = Flowable.fromPublisher(resultingPublisher).test();

      test.assertNotComplete();

      CacheEntry entry1 = new ImmortalCacheEntry(1, 1);
      valuePublisher.onNext(entry1);
      CacheEntry entry2 = new ImmortalCacheEntry(2, 2);
      valuePublisher.onNext(entry2);
      CacheEntry entry3 = new ImmortalCacheEntry(3, 3);
      valuePublisher.onNext(entry3);

      // We should have received 3 entries down now
      test.assertValueCount(3);

      test.assertNotComplete();

      Mockito.verify(segmentConsumer, Mockito.never()).accept(Mockito.any());

      // Finally complete the publisher which will in turn complete any further down the line
      valuePublisher.onComplete();

      test.assertComplete();

      // We still shouldn't be notified since we haven't iterated upon the last entry though
      Mockito.verify(segmentConsumer, Mockito.never()).accept(Mockito.any());

      // Now let our iterate over the entries
      crpd.valueIterated(entry1);
      Mockito.verify(segmentConsumer, Mockito.never()).accept(Mockito.any());
      crpd.valueIterated(entry2);
      Mockito.verify(segmentConsumer, Mockito.never()).accept(Mockito.any());
      crpd.valueIterated(entry3);

      notifySegmentsCompleted.accept(segments);

      // Now that we iterated over entry3 we should be completed!
      ArgumentCaptor<Supplier<PrimitiveIterator.OfInt>> captor = ArgumentCaptor.forClass(Supplier.class);

      Mockito.verify(segmentConsumer, Mockito.times(1)).accept(captor.capture());

      PrimitiveIterator.OfInt completedSegments = captor.getValue().get();

      assertEquals(segments, SmallIntSet.of(completedSegments));
   }

   /**
    * Test to verify when local only stream is used and all entries are published and completed before iteration
    */
   public void testLocalOnlyStreamCompletes() {
      IntSet segments = SmallIntSet.of(1, 4);

      Consumer<Supplier<PrimitiveIterator.OfInt>> segmentConsumer = mock(Consumer.class);
      Consumer<Object> entryConsumer = mock(Consumer.class);
      ConsistentHash ch = mock(ConsistentHash.class);

      CompletionRehashPublisherDecorator<Object> crpd = createDecorator(ch, segments, null, segmentConsumer,
            entryConsumer);

      PublishProcessor<Object> localPublisherProcessor = PublishProcessor.create();

      // This is local only iteration
      Publisher<Object> localPublisher = crpd.decorateLocal(ch, true, segments, localPublisherProcessor);

      simpleAssert(localPublisher, localPublisherProcessor, segmentConsumer, crpd, s -> { }, segments);
   }

   public void testRemoteOnlyStreamCompletes() {
      IntSet segments = SmallIntSet.of(1, 4);

      Consumer<Supplier<PrimitiveIterator.OfInt>> segmentConsumer = mock(Consumer.class);
      Consumer<Object> entryConsumer = mock(Consumer.class);

      CompletionRehashPublisherDecorator<Object> crpd = createDecorator(segmentConsumer, entryConsumer);

      TestRemoteIteratorPublisher<Object> remoteIteratorPublisher = new TestRemoteIteratorPublisher<>();

      // Remote only iteration
      Publisher<Object> resultingPublisher = crpd.decorateRemote(remoteIteratorPublisher);

      simpleAssert(resultingPublisher, remoteIteratorPublisher.publishProcessor(), segmentConsumer, crpd, s ->
            remoteIteratorPublisher.notifyCompleted(s::iterator), segments);
   }

   public void testRemoteAndLocal() {
      IntSet segments = SmallIntSet.of(1, 4);

      Consumer<Supplier<PrimitiveIterator.OfInt>> segmentConsumer = mock(Consumer.class);
      Consumer<Object> entryConsumer = mock(Consumer.class);
      ConsistentHash ch = mock(ConsistentHash.class);

      CompletionRehashPublisherDecorator<Object> crpd = createDecorator(ch, segments, null, segmentConsumer,
            entryConsumer);

      PublishProcessor<Object> localPublisherProcessor = PublishProcessor.create();

      Publisher<Object> localPublisher = crpd.decorateLocal(ch, true, segments,
            localPublisherProcessor);

      simpleAssert(localPublisher, localPublisherProcessor, segmentConsumer, crpd, s -> { }, segments);
      // Reset the mock so remote can test it now
      reset(segmentConsumer);

      TestRemoteIteratorPublisher<Object> remoteIteratorPublisher = new TestRemoteIteratorPublisher<>();

      Publisher<Object> remotePublisher = crpd.decorateRemote(remoteIteratorPublisher);

      simpleAssert(remotePublisher, remoteIteratorPublisher.publishProcessor(), segmentConsumer, crpd, s ->
            remoteIteratorPublisher.notifyCompleted(s::iterator), segments);
   }

   public void testRemoteAndLocalCompleteSameTime() {
      IntSet localSegments = SmallIntSet.of(1, 4);
      IntSet remoteSegments = SmallIntSet.of(2, 3);

      Consumer<Supplier<PrimitiveIterator.OfInt>> segmentConsumer = mock(Consumer.class);
      Consumer<Object> entryConsumer = mock(Consumer.class);
      ConsistentHash ch = mock(ConsistentHash.class);

      CompletionRehashPublisherDecorator<Object> crpd = createDecorator(ch, localSegments, null, segmentConsumer,
            entryConsumer);

      PublishProcessor<Object> localPublisherProcessor = PublishProcessor.create();

      Publisher<Object> localPublisher = crpd.decorateLocal(ch, true, localSegments, localPublisherProcessor);

      TestSubscriber<Object> localSubscriber = Flowable.fromPublisher(localPublisher).test();

      TestRemoteIteratorPublisher<Object> remoteIteratorPublisher = new TestRemoteIteratorPublisher<>();
      PublishProcessor<Object> remotePublisherProcessor = remoteIteratorPublisher.publishProcessor();

      Publisher<Object> remotePublisher = crpd.decorateRemote(remoteIteratorPublisher);

      TestSubscriber<Object> remoteSubscriber = Flowable.fromPublisher(remotePublisher).test();

      localSubscriber.assertNotComplete();
      remoteSubscriber.assertNotComplete();

      // 1 local
      // 2 remote
      // 3 remote
      // 4 local
      // 5 remote
      CacheEntry entry1 = new ImmortalCacheEntry(1, 1);
      localPublisherProcessor.onNext(entry1);
      CacheEntry entry2 = new ImmortalCacheEntry(2, 2);
      remotePublisherProcessor.onNext(entry2);
      CacheEntry entry3 = new ImmortalCacheEntry(3, 3);
      remotePublisherProcessor.onNext(entry3);
      CacheEntry entry4 = new ImmortalCacheEntry(4, 4);
      localPublisherProcessor.onNext(entry4);
      CacheEntry entry5 = new ImmortalCacheEntry(5, 5);
      remotePublisherProcessor.onNext(entry5);

      localSubscriber.assertValueCount(2);
      remoteSubscriber.assertValueCount(3);

      localSubscriber.assertNotComplete();
      remoteSubscriber.assertNotComplete();

      localPublisherProcessor.onComplete();
      remoteIteratorPublisher.notifyCompleted(remoteSegments::iterator);
      remotePublisherProcessor.onComplete();

      localSubscriber.assertComplete();
      remoteSubscriber.assertComplete();

      // Even though the publishers are all complete, we haven't iterated so no segments should be notified yet!
      verify(segmentConsumer, never()).accept(any());

      // Now we finally iterate upon them - note that 4 wasn't iterated upon yet since remote took priority
      crpd.valueIterated(entry1);
      crpd.valueIterated(entry2);
      crpd.valueIterated(entry3);
      crpd.valueIterated(entry5);

      ArgumentCaptor<Supplier<PrimitiveIterator.OfInt>> captor = ArgumentCaptor.forClass(Supplier.class);

      Mockito.verify(segmentConsumer, Mockito.times(1)).accept(captor.capture());

      PrimitiveIterator.OfInt completedSegments = captor.getValue().get();

      assertEquals(remoteSegments, SmallIntSet.of(completedSegments));

      reset(segmentConsumer);

      crpd.valueIterated(entry4);

      Mockito.verify(segmentConsumer, Mockito.times(1)).accept(captor.capture());

      completedSegments = captor.getValue().get();

      assertEquals(localSegments, SmallIntSet.of(completedSegments));
   }
}
