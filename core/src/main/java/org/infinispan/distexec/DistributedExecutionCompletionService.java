package org.infinispan.distexec;

import java.util.Comparator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.infinispan.remoting.transport.Address;

/**
 * A {@link CompletionService} that uses a supplied {@link DistributedExecutorService} to execute
 * tasks. This class arranges that submitted tasks are, upon completion, placed on a queue
 * accessible using <tt>take</tt>. The class is lightweight enough to be suitable for transient use
 * when processing groups of tasks.
 * <p>
 * This class must be used instead of a {@link ExecutorCompletionService} provided from
 * java.util.concurrent package. The {@link ExecutorCompletionService} may not be used since it
 * requires the use of a non serializable RunnableFuture object.
 *
 * @author William Burns
 * @author Vladimir Blagojevic
 */
public class DistributedExecutionCompletionService<V> implements CompletionService<V> {
    protected final DistributedExecutorService executor;
    protected final BlockingQueue<CompletableFuture<V>> completionQueue;

   /**
    * Creates an ExecutorCompletionService using the supplied executor for base task execution and a
    * {@link LinkedBlockingQueue} as a completion queue.
    *
    * @param executor
    *           the executor to use
    * @throws NullPointerException
    *            if executor is <tt>null</tt>
    */
    public DistributedExecutionCompletionService(DistributedExecutorService executor) {
        this(executor, null);
    }

   /**
    * Creates an ExecutorCompletionService using the supplied executor for base task execution and
    * the supplied queue as its completion queue.
    *
    * Note: {@link PriorityBlockingQueue} for completionQueue can only be used with accompanying
    * {@link Comparator} as our internal implementation of {@link Future} for each subtask does not
    * implement Comparable interface. Note that we do not provide any guarantees about which
    * particular internal class implements Future interface and these APIs will remain internal.
    *
    * @param executor
    *           the executor to use
    * @param completionQueue
    *           the queue to use as the completion queue normally one dedicated for use by this
    *           service
    * @throws NullPointerException
    *            if executor is <tt>null</tt>
    */
    public DistributedExecutionCompletionService(DistributedExecutorService executor,
                                     BlockingQueue<CompletableFuture<V>> completionQueue) {
        if (executor == null)
            throw new NullPointerException();
        this.executor = executor;

        if (completionQueue == null) {
            this.completionQueue = new LinkedBlockingQueue<CompletableFuture<V>>();
        }
        else {
            this.completionQueue = completionQueue;
        }
    }

    /**
     * {@inheritDoc CompletionService}
     */
    @Override
    public CompletableFuture<V> submit(Callable<V> task) {
       if (task == null) throw new NullPointerException();
       CompletableFuture<V> f = (CompletableFuture<V>) executor.submit(task);
       return f.whenComplete((v, t) -> completionQueue.add(f));
    }

    /**
     * {@inheritDoc CompletionService}
     */
    @Override
    public CompletableFuture<V> submit(Runnable task, V result) {
       if (task == null) throw new NullPointerException();
       CompletableFuture<V> f = (CompletableFuture<V>) executor.submit(task, result);
       return f.whenComplete((v, t) -> completionQueue.add(f));
    }

    /**
     * {@inheritDoc CompletionService}
     */
    @Override
    public CompletableFuture<V> take() throws InterruptedException {
        return completionQueue.take();
    }

    /**
    * {@inheritDoc CompletionService}
    */
    @Override
    public CompletableFuture<V> poll() {
        return completionQueue.poll();
    }

    /**
     * {@inheritDoc CompletionService}
     */
    @Override
    public CompletableFuture<V> poll(long timeout, TimeUnit unit) throws InterruptedException {
        return completionQueue.poll(timeout, unit);
    }

    public <K> Future<V> submit(Callable<V> task, K... input) {
       CompletableFuture<V> f = executor.submit(task, input);
       return f.whenComplete((v, t) -> completionQueue.add(f));
    }

    public List<CompletableFuture<V>> submitEverywhere(Callable<V> task) {
       List<CompletableFuture<V>> fl = executor.submitEverywhere(task);
       for (Future<V> f : fl) {
          ((CompletableFuture<V>) f).whenComplete((v, t) -> completionQueue.add((CompletableFuture<V>) f));
       }
       return fl;
    }

    public <K> List<CompletableFuture<V>> submitEverywhere(Callable<V> task, K... input) {
       List<CompletableFuture<V>> fl = executor.submitEverywhere(task, input);
       for (CompletableFuture<V> f : fl) {
          f.whenComplete((v, t) -> completionQueue.add(f));
       }
       return fl;
    }

   public <K> CompletableFuture<V> submit(Address target, Callable<V> task) {
      CompletableFuture<V> f = executor.submit(target, task);
      return f.whenComplete((v, t) -> completionQueue.add(f));
   }
}
