package org.infinispan.totalorder;

import org.infinispan.CacheException;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.config.Configuration;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.context.impl.RemoteTxInvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.transaction.LocalTransaction;
import org.infinispan.transaction.RemoteTransaction;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.concurrent.IsolationLevel;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.rhq.helpers.pluginAnnotations.agent.DisplayType;
import org.rhq.helpers.pluginAnnotations.agent.Metric;
import org.rhq.helpers.pluginAnnotations.agent.Operation;
import org.rhq.helpers.pluginAnnotations.agent.Units;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.infinispan.util.Util.prettyPrintGlobalTransaction;

/**
 * this class is responsible to validate transactions in the total order based protocol. It ensures the delivered order
 * and will validate multiple transactions in parallel if they are non conflicting transaction.
 *
 * Date: 1/17/12
 * Time: 1:40 PM
 *
 * @author pruivo
 */
@MBean(objectName = "TotalOrderValidator", description = "Total order validator management")
public class TotalOrderValidator {

    private static final Log log = LogFactory.getLog(TotalOrderValidator.class);

    //map between GlobalTransaction and LocalTransaction. used to sync the threads in remote validation and the
    //transaction execution thread
    private final ConcurrentMap<GlobalTransaction, LocalTransaction> localTransactionMap =
            new ConcurrentHashMap<GlobalTransaction, LocalTransaction>();

    //this two maps are only used in repeatable read with write skew check. however, it isn't implemented yet!
    private final ConcurrentMap<GlobalTransaction, RemoteTxInfo> remoteTransactionMap =
            new ConcurrentHashMap<GlobalTransaction, RemoteTxInfo>();
    private final ConcurrentMap<Object, TxBarrier> keysLocked = new ConcurrentHashMap<Object, TxBarrier>();

    private Configuration configuration;
    private InvocationContextContainer invocationContextContainer;

    //the multithread validation is only possible in repeatable read with write skew, where we can validate non
    //conflicting transactions in parallel
    private volatile boolean needsMultiThreadValidation;
    private volatile ThreadPoolExecutor threadPoolExecutor;

    private boolean trace;
    private boolean info;

    //set the Thread's name in the thread pool
    private static ThreadFactory NAMED_THREAD_FACTORY = new ThreadFactory() {

        private final AtomicLong id = new AtomicLong(0);

        @Override
        public Thread newThread(Runnable runnable) {
            return new Thread(runnable, "Validation-Thread-" + id.getAndIncrement());
        }
    };

    //some profiling information (wasted time in queue and validation duration)
    private final AtomicLong waitTimeInQueue = new AtomicLong(0);
    private final AtomicLong validationDuration = new AtomicLong(0);
    private final AtomicInteger numberOfTxValidated = new AtomicInteger(0);
    private final AtomicLong initializationDuration = new AtomicLong(0);
    private volatile boolean statisticsEnabled;

    @Inject
    public void inject(Configuration configuration, InvocationContextContainer invocationContextContainer) {
        this.configuration = configuration;
        this.invocationContextContainer = invocationContextContainer;
    }

    @Start
    public void start() {
        setLogLevel();
        setStatisticsEnabled(configuration.isExposeJmxStatistics());
        needsMultiThreadValidation = configuration.getIsolationLevel() == IsolationLevel.REPEATABLE_READ &&
                configuration.isWriteSkewCheck();

        if(info) {
            log.infof("Starting Total Order Validator component. using thread pool for validation? %s",
                    needsMultiThreadValidation);
        }

        threadPoolExecutor = createNewThreadPool(configuration.getTOCorePoolSize(),
                configuration.getTOMaximumPoolSize(), configuration.getTOKeepAliveTime(),
                configuration.getTOQueueSize());

        if(info) {
            log.infof("Thread pool size: core=%s, maximum=%s, idleTime=%s",
                    threadPoolExecutor.getCorePoolSize(),
                    threadPoolExecutor.getMaximumPoolSize(),
                    threadPoolExecutor.getKeepAliveTime(TimeUnit.MILLISECONDS));
        }
    }

    @Stop
    public void stop() {
        if(info) {
            log.infof("Stopping Total Order validator component");
        }
        localTransactionMap.clear();
        remoteTransactionMap.clear();
        keysLocked.clear();
        threadPoolExecutor.shutdownNow();
        threadPoolExecutor = null;
    }

    /**
     * Adds a local transaction to the map. Later, it will be notified when the modifications are applied in
     * the data container
     *
     * @param globalTransaction the global transaction
     * @param localTransaction the local transaction
     */
    public void addLocalTransaction(GlobalTransaction globalTransaction, LocalTransaction localTransaction) {
        if(trace) {
            log.tracef("receiving local prepare command. Transaction is %s",
                    prettyPrintGlobalTransaction(globalTransaction));
        }
        localTransactionMap.put(globalTransaction, localTransaction);
    }

    /**
     * put the transaction in the validation queue for further validation. Transactions can be validated in parallel
     * if it is possible
     * @param prepareCommand the command
     * @param ctx the invocation context
     * @param invoker the next Command Interceptor in the chain
     */
    public void validateTransaction(PrepareCommand prepareCommand, TxInvocationContext ctx,
                                    CommandInterceptor invoker) {
        if(trace) {
            log.tracef("validate transaction %s",
                    prettyPrintGlobalTransaction(prepareCommand.getGlobalTransaction()));
        }

        Runnable r;
        if(needsMultiThreadValidation) {
            MultiThreadValidation mtv = new MultiThreadValidation(prepareCommand, ctx, invoker);
            RemoteTxInfo txInfo = mtv.getTxInfo();
            Set<TxBarrier> previousTxs = new HashSet<TxBarrier>();

            //this will collect all the count down latch corresponding to the previous transactions in the queue
            for(Object key : txInfo.keys) {
                TxBarrier prevTx = keysLocked.put(key, txInfo.barrier);
                if(prevTx != null) {
                    previousTxs.add(prevTx);
                }
            }

            //this is invoked with remote context
            txInfo.remoteTransaction = (RemoteTransaction) ctx.getCacheTransaction();

            mtv.setPreviousTransactions(previousTxs);
            remoteTransactionMap.put(prepareCommand.getGlobalTransaction(), txInfo);
            r = mtv;

            if(trace) {
                log.tracef("Transaction [%s] write set is %s", txInfo.barrier.gtx, txInfo.keys);
            }

        } else {
            r = new SingleThreadValidation(prepareCommand, ctx, invoker);
        }
        threadPoolExecutor.execute(r);
    }

    /**
     * this will mark a global transaction as finished. It will be invoked in the processing of the commit command
     * in repeatable read with write skew (not implemented yet!)
     * @param gtx the global transaction
     */
    public void finishTransaction(GlobalTransaction gtx) {
        if(trace) {
            log.tracef("transaction %s is finished", prettyPrintGlobalTransaction(gtx));
        }
        RemoteTxInfo txInfo = remoteTransactionMap.remove(gtx);
        if(txInfo != null) {
            finishTransaction(txInfo.keys, txInfo.barrier);
        } else {
            throw new IllegalStateException("TxInfo can't be null, otherwise can originate deadlocks");
        }
    }

    public void markTransactionForRollback(GlobalTransaction gtx) {
        getOrCreateTxInfo(gtx).markRollbackOnly();
    }

    public void waitForTxPrepared(TxInvocationContext ctx, GlobalTransaction gtx) {
        if(trace) {
            log.tracef("waiting until transaction %s is prepared",
                    prettyPrintGlobalTransaction(gtx));
        }
        RemoteTxInfo txInfo = getOrCreateTxInfo(gtx);
        try {
            txInfo.waitPrepared();

            if (txInfo.remoteTransaction == null) {
                throw new IllegalStateException("Remote Transaction can't be null");
            }
        } catch (InterruptedException e) {
            log.warnf("Timeout received while waiting for the transaction preparing [%s]. continue invocation",
                    prettyPrintGlobalTransaction(gtx));

            if (txInfo.remoteTransaction == null) {
                throw new IllegalStateException("Remote Transaction can't be null");
            }
        } finally {
            if(txInfo.isMarkedForRollback() && txInfo.remoteTransaction != null) {
                txInfo.remoteTransaction.invalidate();
            }
            ((RemoteTxInvocationContext)ctx).setRemoteTransaction(txInfo.remoteTransaction);
            if(trace) {
                log.tracef("waiting time finished for transaction %s",
                        prettyPrintGlobalTransaction(gtx));
            }
        }

    }

    public boolean isTransactionPrepared(GlobalTransaction gtx) {
        return localTransactionMap.containsKey(gtx) || remoteTransactionMap.containsKey(gtx);
    }

    private RemoteTxInfo getOrCreateTxInfo(GlobalTransaction gtx) {
        RemoteTxInfo txInfo = remoteTransactionMap.get(gtx);
        if(txInfo == null) {
            txInfo = new RemoteTxInfo();
            RemoteTxInfo existingTxInfo = remoteTransactionMap.putIfAbsent(gtx, txInfo);
            if (existingTxInfo != null) {
                txInfo = existingTxInfo;
            }
        }
        return txInfo;
    }

    /**
     * creates a new thread pool executor
     * see {@link ThreadPoolExecutor}
     *
     * @param corePoolSize the core pool size
     * @param maxPoolSize the max pool size
     * @param keepAliveTime the keep alive time in milliseconds
     * @param capacity the fixed capacity (used only in repeatable read with write skew)
     * @return the new thread pool
     */
    private ThreadPoolExecutor createNewThreadPool(int corePoolSize, int maxPoolSize, long keepAliveTime,
                                                   int capacity) {
        //only for write skew check
        if (needsMultiThreadValidation) {
            return new ThreadPoolExecutor(corePoolSize, maxPoolSize, keepAliveTime, TimeUnit.MILLISECONDS,
                    new ArrayBlockingQueue<Runnable>(capacity), NAMED_THREAD_FACTORY,
                    new ThreadPoolExecutor.CallerRunsPolicy());
        } else {
            return new ThreadPoolExecutor(1, 1, keepAliveTime, TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<Runnable>(), NAMED_THREAD_FACTORY,
                    new ThreadPoolExecutor.CallerRunsPolicy());
        }
    }

    /**
     * see {@link #finishTransaction(org.infinispan.transaction.xa.GlobalTransaction)}
     *
     * remove the keys from the map (if their didn't change) and release the count down latch, unblocking
     * the next transaction
     *
     * @param keysModified the keys modified by the transaction
     * @param barrier the count down latch corresponding to the transaction
     */
    private void finishTransaction(Set<Object> keysModified, TxBarrier barrier) {
        if (trace) {
            log.tracef("Barrier %s is going to be released", barrier);
        }

        barrier.countDown();
        for(Object key : keysModified) {
            this.keysLocked.remove(key, barrier);
        }
    }

    /**
     * set the boolean trace and info
     */
    private void setLogLevel() {
        trace = log.isTraceEnabled();
        info = log.isInfoEnabled();
    }

    private void updateDurationStats(long creationTime, long validationStartTime, long validationEndTime,
                                     long initializationEndTime) {
        if(statisticsEnabled) {
            //set the profiling information
            waitTimeInQueue.addAndGet(validationStartTime - creationTime);
            initializationDuration.addAndGet(initializationEndTime - validationStartTime);
            validationDuration.addAndGet(validationEndTime - initializationEndTime);
            numberOfTxValidated.incrementAndGet();
        }
    }

    /**
     * this class is used to validate transaction in read committed or repeatable read without write skew.
     */
    private class SingleThreadValidation implements Runnable {

        protected final PrepareCommand prepareCommand;
        private final TxInvocationContext txInvocationContext;
        private final CommandInterceptor invoker;

        private long creationTime = -1;
        private long validationStartTime = -1;
        private long validationEndTime = -1;
        private long initializationEndTime = -1;

        private SingleThreadValidation(PrepareCommand prepareCommand, TxInvocationContext txInvocationContext,
                                       CommandInterceptor invoker) {
            if(prepareCommand == null || txInvocationContext == null || invoker == null) {
                throw new IllegalArgumentException("Arguments must not be null");
            }
            this.prepareCommand = prepareCommand;
            this.txInvocationContext = txInvocationContext;
            this.invoker = invoker;
            this.creationTime = System.nanoTime();
        }

        /**
         * set the initialization of the thread before the validation
         * @throws InterruptedException -- see MultiThreadValidation#initializeValidation
         */
        protected void initializeValidation() throws Exception {
            //single-thread...
            invocationContextContainer.setContext(txInvocationContext);
        }

        /**
         * finishes the transaction, ie, mark the modification as applied and set the result (exception or not)
         * @param result the validation return value
         * @param exception true if the return value is an exception
         */
        protected void finalizeValidation(Object result, boolean exception) {
            GlobalTransaction gtx = prepareCommand.getGlobalTransaction();
            LocalTransaction localTransaction = localTransactionMap.get(gtx);

            if(localTransaction != null) {
                localTransaction.addPrepareResult(result, exception);
                localTransactionMap.remove(gtx);
            }
        }

        @Override
        public void run() {
            validationStartTime = System.nanoTime();
            Object result = null;
            boolean exception = false;
            try {
                if(trace) {
                    log.tracef("Thread %s is validating transaction %s", Thread.currentThread().getName(),
                            prettyPrintGlobalTransaction(prepareCommand.getGlobalTransaction()));
                }
                initializeValidation();
                initializationEndTime = System.nanoTime();

                //invoke next interceptor in the chain
                result = prepareCommand.acceptVisitor(txInvocationContext, invoker);
            } catch (Throwable t) {
                if (initializationEndTime == -1) {
                    initializationEndTime = System.nanoTime();
                }
                result = t;
                exception = true;
            } finally {
                if(trace) {
                    log.tracef("Transaction %s finished validation (%s). Validation result is %s ",
                            prettyPrintGlobalTransaction(prepareCommand.getGlobalTransaction()),
                            (exception ? "failed" : "ok"), (exception ? ((Throwable)result).getMessage() : result));
                }
                finalizeValidation(result, exception);
                validationEndTime = System.nanoTime();
                updateDurationStats(creationTime, validationStartTime, validationEndTime, initializationEndTime);
            }
        }
    }

    /**
     * This class is used to validate transaction in repeatable read with write skew check
     */
    private class MultiThreadValidation extends SingleThreadValidation {

        //to order the transactions
        private final TxBarrier barrier;

        //the set of others transaction's count down latch (it will be unblocked when the transaction finishes)
        private final Set<TxBarrier> previousTransactions;

        private RemoteTxInfo txInfo = null;

        private MultiThreadValidation(PrepareCommand prepareCommand, TxInvocationContext txInvocationContext,
                                      CommandInterceptor invoker) {
            super(prepareCommand, txInvocationContext, invoker);
            this.barrier = new TxBarrier(prepareCommand.getGlobalTransaction());
            this.previousTransactions = new HashSet<TxBarrier>();
        }

        public void setPreviousTransactions(Set<TxBarrier> previousTransactions) {
            this.previousTransactions.addAll(previousTransactions);
        }

        public RemoteTxInfo getTxInfo() {
            if(txInfo == null) {
                txInfo = getOrCreateTxInfo(prepareCommand.getGlobalTransaction());
                txInfo.keys.addAll(prepareCommand.getAffectedKeys());
                txInfo.barrier = barrier;
            }
            return txInfo;
        }

        /**
         * set the initialization of the thread before the validation
         * ensures the validation order in conflicting transactions
         * @throws InterruptedException if this thread was interrupted
         */
        @Override
        protected void initializeValidation() throws Exception {
            String gtx = prettyPrintGlobalTransaction(prepareCommand.getGlobalTransaction());
            super.initializeValidation();

            if(txInfo.isMarkedForRollback()) {
                throw new CacheException("Cannot prepare transaction" + gtx +". it was already marked as rollbacked");
            }

            //just to be safer
            previousTransactions.remove(barrier);

            for (TxBarrier prevTx : previousTransactions) {
                if(trace) {
                    log.tracef("Transaction %s will wait for %s", gtx, prevTx);
                }
                prevTx.await();
            }

            if(txInfo.isMarkedForRollback()) {
                throw new CacheException("Cannot prepare transaction" + gtx + ". it was already marked as rollbacked");
            }
        }

        /**
         * finishes the transaction, ie, mark the modification as applied and set the result (exception or not)
         * invokes the method #finishTransaction if the transaction has the one phase commit set to true
         * @param result the validation return value
         * @param exception true if the return value is an exception
         */
        @Override
        protected void finalizeValidation(Object result, boolean exception) {
            txInfo.markPreparedAndNotify();
            super.finalizeValidation(result, exception);
            if(prepareCommand.isOnePhaseCommit()) {
                finishTransaction(txInfo.keys, txInfo.barrier);
            }
        }
    }

    /**
     * keeps information about a transaction (keys modified and the count down latch)
     * (used in repeatable read with write skew)
     */
    private class RemoteTxInfo {
        public static final byte PREPARED = 1<<1;
        public static final byte ROLLBACK_ONLY = 1<<2;

        Set<Object> keys = new HashSet<Object>();
        TxBarrier barrier;
        byte state;
        RemoteTransaction remoteTransaction;

        private RemoteTxInfo() {
            this.state = 0;
        }

        private void setState(byte b) {
            state |= b;
        }

        private boolean checkState(byte b) {
            return (state & b) != 0;
        }

        private synchronized boolean isMarkedForRollback() {
            return checkState(ROLLBACK_ONLY);
        }

        private synchronized void markPreparedAndNotify() {
            setState(PREPARED);
            this.notifyAll();
        }

        private synchronized void markRollbackOnly() {
            setState(ROLLBACK_ONLY);
        }

        private synchronized boolean waitPrepared() throws InterruptedException {
            if (!checkState(PREPARED)) {
                this.wait();
            }
            return checkState(PREPARED);
        }

    }

    private class TxBarrier extends CountDownLatch {
        private String gtx;

        public TxBarrier(GlobalTransaction gtx) {
            super(1);
            this.gtx = prettyPrintGlobalTransaction(gtx);
        }

        @Override
        public String toString() {
            return "TxBarrier{" +
                    "gtx=" + gtx +
                    "}";
        }
    }


    @ManagedAttribute(description = "The minimum number of threads in the thread pool")
    @Metric(displayName = "Minimum Number of Threads", displayType = DisplayType.DETAIL)
    public int getThreadPoolCoreSize() {
        return threadPoolExecutor.getCorePoolSize();
    }

    @ManagedAttribute(description = "The maximum number of threads in the thread pool")
    @Metric(displayName = "Maximum Number of Threads", displayType = DisplayType.DETAIL)
    public int getThreadPoolMaximumPoolSize() {
        return threadPoolExecutor.getMaximumPoolSize();
    }

    @ManagedAttribute(description = "The keep alive time of an idle thread in the thread pool (milliseconds)")
    @Metric(displayName = "Keep Alive Time of a Idle Thread", units = Units.MILLISECONDS,
            displayType = DisplayType.DETAIL)
    public long getThreadPoolKeepTime() {
        return threadPoolExecutor.getKeepAliveTime(TimeUnit.MILLISECONDS);
    }

    @ManagedAttribute(description = "The percentage of occupation of the queue")
    @Metric(displayName = "Percentage of Occupation of the Queue", units = Units.PERCENTAGE,
            displayType = DisplayType.SUMMARY)
    public double getNumberOfTransactionInPendingQueue() {
        int remainingCapacity = threadPoolExecutor.getQueue().remainingCapacity();
        int actualSize = threadPoolExecutor.getQueue().size();

        double percentage = 0;
        if ((Integer.MAX_VALUE - remainingCapacity) > actualSize) {
            percentage = actualSize * 100.0 / (remainingCapacity + actualSize);
        } else {
            percentage = actualSize * 100.0 / remainingCapacity;
        }

        return percentage > 100 ? 100.0 : percentage;
    }

    @ManagedAttribute(description = "The approximate percentage of active threads in the thread pool")
    @Metric(displayName = "Percentage of Active Threads", units = Units.PERCENTAGE, displayType = DisplayType.SUMMARY)
    public double getPercentageActiveThreads() {
        int max = threadPoolExecutor.getMaximumPoolSize();
        int actual = threadPoolExecutor.getActiveCount();
        double percentage = actual * 100.0 / max;
        return percentage > 100 ? 100.0 : percentage;
    }

    @ManagedAttribute(description = "Average time in the queue before the validation (milliseconds)")
    @Metric(displayName = "Average Waiting Duration In Queue", units = Units.MILLISECONDS,
            displayType = DisplayType.SUMMARY)
    public double getAverageWaitingTimeInQueue() {
        long time = waitTimeInQueue.get();
        int tx = numberOfTxValidated.get();
        if(tx == 0) {
            return 0;
        }
        return (time / tx) / 1000000.0;
    }

    @ManagedAttribute(description = "Average duration of a transaction validation (milliseconds)")
    @Metric(displayName = "Average Validation Duration", units = Units.MILLISECONDS, displayType = DisplayType.SUMMARY)
    public double getAverageValidationDuration() {
        long time = validationDuration.get();
        int tx = numberOfTxValidated.get();
        if(tx == 0) {
            return 0;
        }
        return (time / tx) / 1000000.0;
    }

    @ManagedAttribute(description = "Average duration of a transaction initialization before validation, ie, " +
            "ensuring the order of transactions (milliseconds)")
    @Metric(displayName = "Average Initialization Duration", units = Units.MILLISECONDS,
            displayType = DisplayType.SUMMARY)
    public double getAverageInitializationDuration() {
        long time = initializationDuration.get();
        int tx = numberOfTxValidated.get();
        if(tx == 0) {
            return 0;
        }
        return (time / tx) / 1000000.0;
    }

    @ManagedOperation(description = "Set the minimum number of threads in the thread pool")
    @Operation(displayName = "Set Minimum Number Of Threads")
    public void setThreadPoolCoreSize(int size) {
        if(needsMultiThreadValidation) {
            threadPoolExecutor.setCorePoolSize(size);
        }
    }

    @ManagedOperation(description = "Set the maximum number of threads in the thread pool")
    @Operation(displayName = "Set Maximum Number Of Threads")
    public void setThreadPoolMaximumPoolSize(int size) {
        if(needsMultiThreadValidation) {
            threadPoolExecutor.setMaximumPoolSize(size);
        }
    }

    @ManagedOperation(description = "Set the idle time of a thread in the thread pool (milliseconds)")
    @Operation(displayName = "Set Keep Alive Time of Idle Threads")
    public void setThreadPoolKeepTime(long time) {
        threadPoolExecutor.setKeepAliveTime(time, TimeUnit.MILLISECONDS);
    }

    @ManagedOperation(description = "Resets the statistics")
    public void resetStatistics() {
        waitTimeInQueue.set(0);
        validationDuration.set(0);
        numberOfTxValidated.set(0);
        initializationDuration.set(0);
    }

    @ManagedAttribute(description = "Show it the gathering of statistics is enabled")
    public boolean isStatisticsEnabled() {
        return statisticsEnabled;
    }

    @ManagedOperation(description = "Enables or disables the gathering of statistics by this component")
    public void setStatisticsEnabled(boolean statisticsEnabled) {
        this.statisticsEnabled = statisticsEnabled;
    }
}
