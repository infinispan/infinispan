package org.infinispan.distexec;

import java.util.Comparator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.infinispan.commons.util.concurrent.FutureListener;
import org.infinispan.commons.util.concurrent.NotifyingFuture;
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
    protected final BlockingQueue<NotifyingFuture<V>> completionQueue;
    protected final QueueingListener listener;

    protected class QueueingListener implements FutureListener<V> {
        @Override
        public void futureDone(Future<V> future) {
            // This is a safe cast since this listener should only used
            // in this class
            completionQueue.add((NotifyingFuture<V>)future);
        }

    }

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
                                     BlockingQueue<NotifyingFuture<V>> completionQueue) {
        if (executor == null)
            throw new NullPointerException();
        this.executor = executor;

        if (completionQueue == null) {
            this.completionQueue = new LinkedBlockingQueue<NotifyingFuture<V>>();
        }
        else {
            this.completionQueue = completionQueue;
        }
        this.listener = new QueueingListener();
    }

    /**
     * {@inheritDoc CompletionService}
     * <p>
     * This future object may not be used as a NotifyingFuture.  That is because
     * internally this class sets the listener to provide ability to add to the queue.
     */
    @Override
    public Future<V> submit(Callable<V> task) {
        if (task == null) throw new NullPointerException();
        NotifyingFuture<V> f = (NotifyingFuture<V>) executor.submit(task);
        f.attachListener(listener);
        return f;
    }

    /**
     * {@inheritDoc CompletionService}
     * <p>
     * This future object may not be used as a NotifyingFuture.  That is because
     * internally this class sets the listener to provide ability to add to the queue.
     */
    @Override
    public Future<V> submit(Runnable task, V result) {
        if (task == null) throw new NullPointerException();
        NotifyingFuture<V> f = (NotifyingFuture<V>) executor.submit(task, result);
        f.attachListener(listener);
        return f;
    }

    /**
     * {@inheritDoc CompletionService}
     * <p>
     * This future may safely be used as a NotifyingFuture if desired.  This
     * is because if it tries to set a listener it will be called immediately
     * since the task has already been completed.
     */
    @Override
    public NotifyingFuture<V> take() throws InterruptedException {
        return completionQueue.take();
    }

    /**
    * {@inheritDoc CompletionService}
    * <p>
    * This future may safely be used as a NotifyingFuture if desired.  This
    * is because if it tries to set a listener it will be called immediately
    * since the task has already been completed.
    */
    @Override
    public NotifyingFuture<V> poll() {
        return completionQueue.poll();
    }

    /**
     * {@inheritDoc CompletionService}
     * <p>
     * This future may safely be used as a NotifyingFuture if desired.  This
     * is because if it tries to set a listener it will be called immediately
     * since the task has already been completed.
     */
    @Override
    public NotifyingFuture<V> poll(long timeout, TimeUnit unit) throws InterruptedException {
        return completionQueue.poll(timeout, unit);
    }

    public <K> Future<V> submit(Callable<V> task, K... input) {
       NotifyingFuture<V> f = executor.submit(task, input);
       f.attachListener(listener);
       return f;
    }

    public List<Future<V>> submitEverywhere(Callable<V> task) {
       List<Future<V>> fl = executor.submitEverywhere(task);
       for (Future<V> f : fl) {
          ((NotifyingFuture<V>) f).attachListener(listener);
       }
       return fl;
    }

    public <K> List<Future<V>> submitEverywhere(Callable<V> task, K... input) {
       List<Future<V>> fl = executor.submitEverywhere(task, input);
       for (Future<V> f : fl) {
          ((NotifyingFuture<V>) f).attachListener(listener);
       }
       return fl;
    }

   public <K> Future<V> submit(Address target, Callable<V> task) {
      NotifyingFuture<V> f = executor.submit(target, task);
      f.attachListener(listener);
      return f;
   }
}
