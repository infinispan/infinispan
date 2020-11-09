package org.infinispan.persistence.sifs;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.concurrent.TimeUnit;

/**
 * This benchmark tests the performance of {@link SyncProcessingQueue}
 *
 * @author Francesco Nigro &lt;fnigro@redhat.com&gt;
 */

@State(Scope.Benchmark)
@Fork(2)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 4, time = 1)
@Measurement(iterations = 8, time = 1)
public class SyncProcessingQueueBenchmark {

    @Param({ "0", "10", "100" })
    public int producerDelay;
    @Param({ "0", "10", "100" })
    public int consumerDelay;
    private static final Integer ITEM = 0;
    private SyncProcessingQueue<Integer> queue;
    private Thread consumerThread;

    @Setup
    public void init(Blackhole bh) throws InterruptedException {
        queue = new SyncProcessingQueue<>();
        this.consumerThread = new Thread(() -> {
            final Thread currentThread = Thread.currentThread();
            final SyncProcessingQueue<Integer> queue = this.queue;
            while (!currentThread.isInterrupted()) {
                Integer value = queue.pop();
                if (value != null) {
                    bh.consume(value);
                    final int delay = this.consumerDelay;
                    if (delay > 0) {
                        Blackhole.consumeCPU(delay);
                    }
                    queue.notifyNoWait();
                } else {
                    queue.notifyAndWait();
                }
            }
        });
        this.consumerThread.start();
        queue.pushAndWait(ITEM);
    }

    @Benchmark
    public void pushAndWait() throws InterruptedException {
        final int delay = this.producerDelay;
        if (delay > 0) {
            Blackhole.consumeCPU(delay);
        }
        queue.pushAndWait(ITEM);
    }

    @Benchmark
    @Threads(2)
    public void pushAndWait2() throws InterruptedException {
        final int delay = this.producerDelay;
        if (delay > 0) {
            Blackhole.consumeCPU(delay);
        }
        queue.pushAndWait(ITEM);
    }

    @TearDown
    public void clear() {
        synchronized (queue) {
            consumerThread.interrupt();
            queue.notifyError();
        }
    }

}
