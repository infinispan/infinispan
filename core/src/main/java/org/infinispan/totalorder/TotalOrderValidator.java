package org.infinispan.totalorder;

import org.infinispan.CacheException;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.config.Configuration;
import org.infinispan.container.versioning.EntryVersionsMap;
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
import org.infinispan.util.Util;
import org.infinispan.util.concurrent.IsolationLevel;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.rhq.helpers.pluginAnnotations.agent.DisplayType;
import org.rhq.helpers.pluginAnnotations.agent.Metric;
import org.rhq.helpers.pluginAnnotations.agent.Operation;
import org.rhq.helpers.pluginAnnotations.agent.Units;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

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
    private final Map<GlobalTransaction, LocalTransaction> localTransactionMap = new HashMap<GlobalTransaction, LocalTransaction>();

    //this two maps are only used in repeatable read with write skew check. however, it isn't implemented yet!
    private final ConcurrentHashMap<GlobalTransaction, TxInfo> remoteTransactionMap = new ConcurrentHashMap<GlobalTransaction, TxInfo>();
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
    private volatile boolean statisticsEnabled;

    public TotalOrderValidator() {
        threadPoolExecutor = createNewThreadPool();
    }

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

        if(needsMultiThreadValidation) {
            //create a thread pool with the parameters in configuration file
            threadPoolExecutor.setCorePoolSize(configuration.getTOCorePoolSize());
            threadPoolExecutor.setMaximumPoolSize(configuration.getTOMaximumPoolSize());
            threadPoolExecutor.setKeepAliveTime(configuration.getTOKeepAliveTime(), TimeUnit.MILLISECONDS);
        } else {
            //create a thread pool with one thread only
            threadPoolExecutor.setCorePoolSize(1);
            threadPoolExecutor.setMaximumPoolSize(1);
            threadPoolExecutor.setKeepAliveTime(configuration.getTOKeepAliveTime(), TimeUnit.MILLISECONDS);
        }

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
        threadPoolExecutor = createNewThreadPool();
    }

    /**
     * Adds a local transaction to the map. Later, it will be notified when the modifications are applied in
     * the data container
     * @param prepareCommand the prepare command (contains the GlobalTransaction instance)
     * @param ctx the invocation context (local and transactional) (contains the LocalTransaction)
     */
    public void addLocalTransaction(PrepareCommand prepareCommand, TxInvocationContext ctx) {
        if(trace) {
            log.tracef("receiving local prepare command. Transaction is %s",
                    Util.prettyPrintGlobalTransaction(prepareCommand.getGlobalTransaction()));
        }
        localTransactionMap.put(prepareCommand.getGlobalTransaction(), (LocalTransaction) ctx.getCacheTransaction());
    }

    /**
     * put the transaction in the validation queue for further validation. Transactions can be validated in parallel
     * if it is possible
     * @param prepareCommand the command
     * @param ctx the invocation context
     * @param invoker the next Command Interceptor in the chain
     */
    public void validateTransaction(PrepareCommand prepareCommand, TxInvocationContext ctx, CommandInterceptor invoker) {
        if(trace) {
            log.tracef("validate transaction %s",
                    Util.prettyPrintGlobalTransaction(prepareCommand.getGlobalTransaction()));
        }

        Runnable r;
        if(needsMultiThreadValidation) {
            MultiThreadValidation mtv = new MultiThreadValidation(prepareCommand, ctx, invoker);
            TxInfo txInfo = mtv.getTxInfo();
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
            log.tracef("transaction %s is finished", Util.prettyPrintGlobalTransaction(gtx));
        }
        TxInfo txInfo = remoteTransactionMap.remove(gtx);
        if(txInfo != null) {
            finishTransaction(txInfo.keys, txInfo.barrier);
        }
    }

    public void markTransactionForRollback(GlobalTransaction gtx) {
        getOrCreateTxInfo(gtx).markRollbackOnly();
    }

    public void waitForTxPrepared(TxInvocationContext ctx, GlobalTransaction gtx) {
        if(trace) {
            log.tracef("waiting until transaction %s is prepared",
                    Util.prettyPrintGlobalTransaction(gtx));
        }
        TxInfo txInfo = getOrCreateTxInfo(gtx);
        try {
            txInfo.waitPrepared(configuration.getSyncReplTimeout());
        } catch (InterruptedException e) {
            log.warnf("Timeout received while waiting for the transaction preparing [%s]. continue invocation",
                    Util.prettyPrintGlobalTransaction(gtx));
        } finally {
            if(txInfo.isMarkedForRollback() && txInfo.remoteTransaction != null) {
                txInfo.remoteTransaction.invalidate();
            }
            ((RemoteTxInvocationContext)ctx).setRemoteTransaction(txInfo.remoteTransaction);
            if(trace) {
                log.tracef("waiting time finished for transaction %s",
                        Util.prettyPrintGlobalTransaction(gtx));
            }
        }

    }

    public boolean isTransactionPrepared(GlobalTransaction gtx) {
        return localTransactionMap.containsKey(gtx) || remoteTransactionMap.containsKey(gtx);
    }

    private TxInfo getOrCreateTxInfo(GlobalTransaction gtx) {
        TxInfo txInfo = remoteTransactionMap.get(gtx);
        if(txInfo == null) {
            txInfo = new TxInfo();
            TxInfo existingTxInfo = remoteTransactionMap.putIfAbsent(gtx, txInfo);
            if (existingTxInfo != null) {
                txInfo = existingTxInfo;
            }
        }
        return txInfo;
    }

    /**
     * creates a new thread pool executor
     * @return the new thread pool 
     */
    private static ThreadPoolExecutor createNewThreadPool() {
        return new ThreadPoolExecutor(1, 1, 1000, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(), NAMED_THREAD_FACTORY,
                new ThreadPoolExecutor.CallerRunsPolicy());
    }

    /**
     * see #finishTransaction(GlobalTransaction)
     * remove the keys from the map (if their didn't change) and release the count down latch, unblocking
     * the next transaction
     * @param keysModified the keys modified by the transaction
     * @param barrier the count down latch corresponding to the transaction
     */
    private void finishTransaction(Set<Object> keysModified, TxBarrier barrier) {
        if (trace) {
            log.tracef("Barrier %s is going to be released", barrier);
        }
        for(Object key : keysModified) {
            this.keysLocked.remove(key, barrier);
        }
        barrier.countDown();
    }

    /**
     * set the boolean trace and info
     */
    private void setLogLevel() {
        trace = log.isTraceEnabled();
        info = log.isInfoEnabled();
    }

    /**
     * this class is used to validate transaction in read committed or repeatable read without write skew.
     */
    private class SingleThreadValidation implements Runnable {

        protected final PrepareCommand prepareCommand;
        private final TxInvocationContext txInvocationContext;
        private final CommandInterceptor invoker;

        private long creationTime;
        private long validationStartTime;
        private long validatinEndTime;

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
         * @return the local transaction
         */
        protected LocalTransaction finalizeValidation(Object result, boolean exception) {
            LocalTransaction localTransaction = localTransactionMap.remove(prepareCommand.getGlobalTransaction());
            if(localTransaction != null) {
                localTransaction.addPrepareResult(result, exception);
            }
            return localTransaction;
        }

        @Override
        public void run() {
            validationStartTime = System.nanoTime();
            Object result = null;
            boolean exception = false;
            try {
                if(trace) {
                    log.tracef("Thread %s is validating transaction %s", Thread.currentThread().getName(),
                            Util.prettyPrintGlobalTransaction(prepareCommand.getGlobalTransaction()));
                }
                initializeValidation();
                //invoke next interceptor in the chain
                result = prepareCommand.acceptVisitor(txInvocationContext, invoker);
            } catch (Throwable t) {
                result = t;
                exception = true;
            } finally {
                if(trace) {
                    log.tracef("Transaction %s finished validation (%s). Validation result is %s ",
                            Util.prettyPrintGlobalTransaction(prepareCommand.getGlobalTransaction()),
                            (exception ? "failed" : "ok"), (exception ? ((Throwable)result).getMessage() : result));
                }
                finalizeValidation(result, exception);
                validatinEndTime = System.nanoTime();
            }

            if(statisticsEnabled) {
                //set the profiling information
                waitTimeInQueue.addAndGet(validationStartTime - creationTime);
                validationDuration.addAndGet(validatinEndTime - validationStartTime);
                numberOfTxValidated.incrementAndGet();
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

        private TxInfo txInfo = null;

        private MultiThreadValidation(PrepareCommand prepareCommand, TxInvocationContext txInvocationContext,
                                      CommandInterceptor invoker) {
            super(prepareCommand, txInvocationContext, invoker);
            this.barrier = new TxBarrier(prepareCommand.getGlobalTransaction(), 1);
            this.previousTransactions = new HashSet<TxBarrier>();
        }

        public void setPreviousTransactions(Set<TxBarrier> previousTransactions) {
            this.previousTransactions.addAll(previousTransactions);
        }

        public TxInfo getTxInfo() {
            if(txInfo == null) {
                txInfo = new TxInfo(prepareCommand.getAffectedKeys(), barrier);
                TxInfo existingTxInfo = remoteTransactionMap.putIfAbsent(prepareCommand.getGlobalTransaction(), txInfo);
                if(existingTxInfo != null) {
                    existingTxInfo.keys = prepareCommand.getAffectedKeys();
                    existingTxInfo.barrier = barrier;
                    txInfo = existingTxInfo;
                }
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
            String gtx = Util.prettyPrintGlobalTransaction(prepareCommand.getGlobalTransaction());
            super.initializeValidation();

            if(txInfo.isMarkedForRollback()) {
                throw new CacheException("Cannot prepare transaction" + gtx +". it was already marked as rollbacked");
            }

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
        protected LocalTransaction finalizeValidation(Object result, boolean exception) {
            txInfo.markPreparedAndNotify();
            LocalTransaction localTransaction = super.finalizeValidation(result, exception);
            if(prepareCommand.isOnePhaseCommit()) {
                finishTransaction(prepareCommand.getAffectedKeys(), barrier);
            } else if(result instanceof EntryVersionsMap) {
                if (localTransaction != null) {
                    //put in local transaction... this way, it passes the new version to the commit command
                    localTransaction.setUpdatedEntryVersions((EntryVersionsMap) result);
                }
            }
            return localTransaction;
        }
    }

    /**
     * keeps information about a transaction (keys modified and the count down latch)
     * (used in repeatable read with write skew)
     */
    private class TxInfo {
        public static final byte PREPARED = 1<<1;
        public static final byte ROLLBACK_ONLY = 1<<2;

        Set<Object> keys = new HashSet<Object>();
        TxBarrier barrier;
        byte state;
        RemoteTransaction remoteTransaction;

        private TxInfo() {
            this.state = 0;
        }

        private TxInfo(Set<Object> keys, TxBarrier barrier) {
            this.keys = keys;
            this.barrier = barrier;
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

        private synchronized boolean waitPrepared(long timeout) throws InterruptedException {
            if (!checkState(PREPARED)) {
                this.wait(timeout);
            }
            return checkState(PREPARED);
        }
    }

    private class TxBarrier extends CountDownLatch {
        private String gtx;

        public TxBarrier(GlobalTransaction gtx, int i) {
            super(i);
            this.gtx = Util.prettyPrintGlobalTransaction(gtx);
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
    @Metric(displayName = "Keep Alive Time of a Idle Thread", units = Units.MILLISECONDS, displayType = DisplayType.DETAIL)
    public long getThreadPoolKeepTime() {
        return threadPoolExecutor.getKeepAliveTime(TimeUnit.MILLISECONDS);
    }

    @ManagedAttribute(description = "The number of transactions waiting validation")
    @Metric(displayName = "Number of Pending Transactions", displayType = DisplayType.SUMMARY)
    public int getNumberOfTransactionInPendingQueue() {
        return threadPoolExecutor.getQueue().size();
    }

    @ManagedAttribute(description = "The approximate number of active threads in the thread pool")
    @Metric(displayName = "Approximate Number of Active Threads", displayType = DisplayType.SUMMARY)
    public int getThreadPoolActiveThreads() {
        return threadPoolExecutor.getActiveCount();
    }

    @ManagedAttribute(description = "Average time in the queue before the validation (milliseconds)")
    @Metric(displayName = "Average Time In Queue", units = Units.MILLISECONDS, displayType = DisplayType.SUMMARY)
    public double getAverageWaitingTimeInQueue() {
        long time = waitTimeInQueue.get();
        int tx = numberOfTxValidated.get();
        if(tx == 0) {
            return 0;
        }
        return (time / tx) / 1000000.0;
    }

    @ManagedAttribute(description = "Average duration of a transaction validation (milliseconds)")
    @Metric(displayName = "Average Validation Time", units = Units.MILLISECONDS, displayType = DisplayType.SUMMARY)
    public double getAverageValidationDuration() {
        long time = validationDuration.get();
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
