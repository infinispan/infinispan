package org.infinispan.distexec;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.infinispan.util.concurrent.FutureListener;
import org.infinispan.util.concurrent.NotifyingFuture;

/**
 * A {@link CompletionService} that uses a supplied {@link DistributedExecutorService}
 * to execute tasks.  This class arranges that submitted tasks are,
 * upon completion, placed on a queue accessible using <tt>take</tt>.
 * The class is lightweight enough to be suitable for transient use
 * when processing groups of tasks.
 * <p>
 * This class must be used instead of a {@link ExecutorCompletionService}
 * provided from java.util.concurrent package.  The 
 * {@link ExecutorCompletionService} may not be used since it requires the use
 * of a non serializable RunnableFuture object.  Since a ExecutionService
 * may only be used with serializable request objects, this class must be used
 * instead.
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
     * Creates an ExecutorCompletionService using the supplied
     * executor for base task execution and a
     * {@link LinkedBlockingQueue} as a completion queue.
     *
     * @param executor the executor to use
     * @throws NullPointerException if executor is <tt>null</tt>
     */
    public DistributedExecutionCompletionService(DistributedExecutorService executor) {
        this(executor, null, null);
    }

    /**
     * Creates an ExecutorCompletionService using the supplied
     * executor for base task execution and the supplied queue as its
     * completion queue.
     *
     * @param executor the executor to use
     * @param completionQueue the queue to use as the completion queue
     * normally one dedicated for use by this service
     * @throws NullPointerException if executor is <tt>null</tt>
     */
    public DistributedExecutionCompletionService(DistributedExecutorService executor,
                                     BlockingQueue<NotifyingFuture<V>> completionQueue) {
        this(executor, completionQueue, null);
    }
    
    /**
     * This constructor is here if someone wants to override this class and 
     * provide their own QueueingListener to possibly listen in on futures
     * being finished
     * @param executor the executor to use
     * @param completionQueue the queue to use as the completion queue
     * normally one dedicated for use by this service
     * @param listener the listener to notify.  To work properly this listner
     *        should at minimum call the super.futureDone or else this
     *        completion service may not work correctly.
     * @throws NullPointerException if executor is <tt>null</tt>
     */
    protected DistributedExecutionCompletionService(DistributedExecutorService executor,
                                     BlockingQueue<NotifyingFuture<V>> completionQueue,
                                     QueueingListener listener) {
        if (executor == null)
            throw new NullPointerException();
        this.executor = executor;
        
        if (completionQueue == null) {
            this.completionQueue = new LinkedBlockingQueue<NotifyingFuture<V>>();
        }
        else {
            this.completionQueue = completionQueue;
        }
        
        if (listener == null) {
            this.listener = new QueueingListener();
        }
        else {
            this.listener = listener;
        }
    }

    /**
     * {@inheritDoc CompletionService}
     * <p>
     * This future object may not be used as a NotifyingFuture.  That is because
     * internally this class sets the listener to provide ability to add to the queue.
     */
    public Future<V> submit(Callable<V> task) {
        if (task == null) throw new NullPointerException();
        NotifyingFuture<V> f = executor.submit(task);
        f.attachListener(listener);
        return f;
    }

    /**
     * {@inheritDoc CompletionService}
     * <p>
     * This future object may not be used as a NotifyingFuture.  That is because
     * internally this class sets the listener to provide ability to add to the queue.
     */
    public Future<V> submit(Runnable task, V result) {
        if (task == null) throw new NullPointerException();
        NotifyingFuture<V> f = executor.submit(task, result);
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
    public NotifyingFuture<V> poll(long timeout, TimeUnit unit) throws InterruptedException {
        return completionQueue.poll(timeout, unit);
    }

}
