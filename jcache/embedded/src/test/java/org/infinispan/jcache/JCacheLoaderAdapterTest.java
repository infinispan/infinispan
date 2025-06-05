package org.infinispan.jcache;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

import org.infinispan.commons.time.TimeService;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.jcache.embedded.JCacheLoaderAdapter;
import org.infinispan.jcache.util.InMemoryJCacheLoader;
import org.infinispan.marshall.TestObjectStreamMarshaller;
import org.infinispan.marshall.persistence.impl.MarshalledEntryFactoryImpl;
import org.infinispan.persistence.DummyInitializationContext;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.persistence.spi.MarshallableEntryFactory;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.util.EmbeddedTimeService;
import org.infinispan.util.concurrent.BlockingManager;
import org.reactivestreams.Publisher;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Unit tests for {@link JCacheLoaderAdapter}.
 *
 * @author Roman Chigvintsev
 */
@Test(groups = "unit", testName = "jcache.JCacheLoaderAdapterTest")
public class JCacheLoaderAdapterTest extends AbstractInfinispanTest {
    private static TestObjectStreamMarshaller marshaller;
    private static InitializationContext ctx;

    private JCacheLoaderAdapter<Integer, String> adapter;

    @BeforeClass
    public static void setUpClass() {
        TimeService timeService = new EmbeddedTimeService();
        marshaller = new TestObjectStreamMarshaller();
        MarshallableEntryFactory marshalledEntryFactory = new MarshalledEntryFactoryImpl(marshaller);
        ctx = new DummyInitializationContext() {
            @Override
            public TimeService getTimeService() {
                return timeService;
            }

            @Override
            public MarshallableEntryFactory getMarshallableEntryFactory() {
                return marshalledEntryFactory;
            }

            @Override
            public BlockingManager getBlockingManager() {
                return new BlockingManager() {
                    @Override
                    public CompletionStage<Void> runBlocking(Runnable runnable, Object traceId) {
                        runnable.run();
                        return CompletableFutures.completedNull();
                    }

                    @Override
                    public <E> CompletionStage<Void> subscribeBlockingConsumer(Publisher<E> publisher, Consumer<E> consumer, Object traceId) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public <T, A, R> CompletionStage<R> subscribeBlockingCollector(Publisher<T> publisher, Collector<? super T, A, R> collector, Object traceId) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public <V> CompletionStage<V> supplyBlocking(Supplier<V> supplier, Object traceId) {
                        return CompletableFuture.completedFuture(supplier.get());
                    }

                    @Override
                    public <I, O> CompletionStage<O> handleBlocking(CompletionStage<? extends I> stage, BiFunction<? super I, Throwable, ? extends O> function, Object traceId) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public <I> CompletionStage<Void> thenRunBlocking(CompletionStage<? extends I> stage, Runnable runnable, Object traceId) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public <I, O> CompletionStage<O> thenApplyBlocking(CompletionStage<? extends I> stage, Function<? super I, ? extends O> function, Object traceId) {
                        return CompletableFutures.completedNull();
                    }

                    @Override
                    public <I, O> CompletionStage<O> thenComposeBlocking(CompletionStage<? extends I> stage, Function<? super I, ? extends CompletionStage<O>> function, Object traceId) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public <V> CompletionStage<V> whenCompleteBlocking(CompletionStage<V> stage, BiConsumer<? super V, ? super Throwable> biConsumer, Object traceId) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public <V> CompletionStage<V> continueOnNonBlockingThread(CompletionStage<V> delay, Object traceId) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public Executor nonBlockingExecutor() {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public <V> Publisher<V> blockingPublisher(Publisher<V> publisher) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public <V> CompletionStage<Void> blockingPublisherToVoidStage(Publisher<V> publisher, Object traceId) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public Executor asExecutor(String name) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public BlockingExecutor limitedBlockingExecutor(String name, int concurrency) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public <V> ScheduledBlockingCompletableStage<V> scheduleRunBlocking(Supplier<V> supplier, long delay, TimeUnit unit, Object traceId) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public ScheduledFuture<Void> scheduleRunBlockingAtFixedRate(Runnable runnable, long initialDelay, long period, TimeUnit unit, Object traceId) {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        };
    }

    @AfterClass
    public static void tearDownClass() {
        marshaller.stop();
    }

    @BeforeMethod
    public void setUpMethod() {
        adapter = new JCacheLoaderAdapter<>();
        adapter.start(ctx);
        adapter.setCacheLoader(new InMemoryJCacheLoader<Integer, String>().store(1, "v1").store(2, "v2"));
    }

    public void testLoad() throws ExecutionException, InterruptedException {
        assertNull(adapter.load(0, 0).toCompletableFuture().get());

        MarshallableEntry v1Entry = adapter.load(0, 1).toCompletableFuture().get();

        assertNotNull(v1Entry);
        assertEquals(1, v1Entry.getKey());
        assertEquals("v1", v1Entry.getValue());

        MarshallableEntry v2Entry = adapter.load(0, 2).toCompletableFuture().get();

        assertNotNull(v2Entry);
        assertEquals(2, v2Entry.getKey());
        assertEquals("v2", v2Entry.getValue());
    }
}
