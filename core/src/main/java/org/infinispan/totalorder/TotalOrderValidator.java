package org.infinispan.totalorder;

import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.config.Configuration;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.transaction.LocalTransaction;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.concurrent.IsolationLevel;
import org.rhq.helpers.pluginAnnotations.agent.Metric;
import org.rhq.helpers.pluginAnnotations.agent.Operation;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Date: 1/17/12
 * Time: 1:40 PM
 *
 * @author pruivo
 */
@MBean(description = "Total order validator management")
public class TotalOrderValidator {

    private final Map<GlobalTransaction, LocalTransaction> localTransactionMap = new HashMap<GlobalTransaction, LocalTransaction>();
    private final Map<GlobalTransaction, TxInfo> remoteTransactionMap = new HashMap<GlobalTransaction, TxInfo>();
    private final ConcurrentMap<Object, CountDownLatch> keysLocked = new ConcurrentHashMap<Object, CountDownLatch>();

    private Configuration configuration;
    private InvocationContextContainer invocationContextContainer;
    private volatile boolean needsMultiThreadValidation;
    private volatile ThreadPoolExecutor threadPoolExecutor;

    private static ThreadFactory NAMED_THREAD_FACTORY = new ThreadFactory() {

        private final AtomicLong id = new AtomicLong(0);

        @Override
        public Thread newThread(Runnable runnable) {
            return new Thread(runnable, "Validation-Thread-" + id.getAndIncrement());
        }
    };

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
        needsMultiThreadValidation = configuration.getIsolationLevel() == IsolationLevel.REPEATABLE_READ &&
                configuration.isWriteSkewCheck();

        if(needsMultiThreadValidation) {
            threadPoolExecutor.setCorePoolSize(configuration.getTOCorePoolSize());
            threadPoolExecutor.setMaximumPoolSize(configuration.getTOMaximumPoolSize());
            threadPoolExecutor.setKeepAliveTime(configuration.getTOKeepAliveTime(), TimeUnit.MILLISECONDS);
        } else {
            threadPoolExecutor.setCorePoolSize(1);
            threadPoolExecutor.setMaximumPoolSize(1);
            threadPoolExecutor.setKeepAliveTime(configuration.getTOKeepAliveTime(), TimeUnit.MILLISECONDS);
        }
    }

    @Stop
    public void stop() {
        localTransactionMap.clear();
        remoteTransactionMap.clear();
        keysLocked.clear();
        threadPoolExecutor.shutdownNow();
        threadPoolExecutor = createNewThreadPool();
    }

    public void addLocalTransaction(PrepareCommand prepareCommand, TxInvocationContext ctx) {
        localTransactionMap.put(prepareCommand.getGlobalTransaction(), (LocalTransaction) ctx.getCacheTransaction());
    }

    public void validateTransaction(PrepareCommand prepareCommand, TxInvocationContext ctx, CommandInterceptor invoker) {
        Runnable r;
        if(needsMultiThreadValidation) {
            MultiThreadValidation mtv = new MultiThreadValidation(prepareCommand, ctx, invoker);
            TxInfo txInfo = mtv.getTxInfo();
            Set<CountDownLatch> previousTxs = new HashSet<CountDownLatch>();

            for(Object key : txInfo.keys) {
                CountDownLatch prevTx = keysLocked.put(key, txInfo.barrier);
                if(prevTx != null) {
                    previousTxs.add(prevTx);
                }
            }

            mtv.setPreviousTransactions(previousTxs);
            remoteTransactionMap.put(prepareCommand.getGlobalTransaction(), txInfo);
            r = mtv;

        } else {
            r = new SingleThreadValidation(prepareCommand, ctx, invoker);
        }
        threadPoolExecutor.execute(r);

    }

    public void finishTransaction(GlobalTransaction gtx) {
        TxInfo txInfo = remoteTransactionMap.remove(gtx);
        if(txInfo != null) {
            finishTransaction(txInfo.keys, txInfo.barrier);
        }
    }

    private static ThreadPoolExecutor createNewThreadPool() {
        return new ThreadPoolExecutor(1, 1, 1000, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(), NAMED_THREAD_FACTORY,
                new ThreadPoolExecutor.CallerRunsPolicy());
    }

    private void finishTransaction(Set<Object> keysModified, CountDownLatch barrier) {
        for(Object key : keysModified) {
            this.keysLocked.remove(key, barrier);
        }
        barrier.countDown();
    }

    private class SingleThreadValidation implements Runnable {

        protected final PrepareCommand prepareCommand;
        private final TxInvocationContext txInvocationContext;
        private final CommandInterceptor invoker;

        private SingleThreadValidation(PrepareCommand prepareCommand, TxInvocationContext txInvocationContext,
                                       CommandInterceptor invoker) {
            if(prepareCommand == null || txInvocationContext == null || invoker == null) {
                throw new IllegalArgumentException("Arguments must not be null");
            }
            this.prepareCommand = prepareCommand;
            this.txInvocationContext = txInvocationContext;
            this.invoker = invoker;
        }

        protected void initializeValidation() throws InterruptedException {
            //single-thread...
            invocationContextContainer.setContext(txInvocationContext);
        }

        protected void finalizeValidation(Object result, boolean exception) {
            LocalTransaction localTransaction = localTransactionMap.remove(prepareCommand.getGlobalTransaction());
            if(localTransaction != null) {
                localTransaction.addPrepareResult(result, exception);
            }
        }

        @Override
        public void run() {
            Object result = null;
            boolean exception = false;
            try {
                initializeValidation();
                result = prepareCommand.acceptVisitor(txInvocationContext, invoker);
            } catch (Throwable t) {
                result = t;
                exception = true;
            } finally {
                finalizeValidation(result, exception);
            }
        }
    }

    private class MultiThreadValidation extends SingleThreadValidation {

        private final CountDownLatch barrier;
        private final Set<CountDownLatch> previousTransactions;

        private MultiThreadValidation(PrepareCommand prepareCommand, TxInvocationContext txInvocationContext, CommandInterceptor invoker) {
            super(prepareCommand, txInvocationContext, invoker);
            this.barrier = new CountDownLatch(1);
            this.previousTransactions = new HashSet<CountDownLatch>();
        }

        public void setPreviousTransactions(Set<CountDownLatch> previousTransactions) {
            this.previousTransactions.addAll(previousTransactions);
        }

        public TxInfo getTxInfo() {
            return new TxInfo(prepareCommand.getAffectedKeys(), barrier);
        }

        @Override
        protected void initializeValidation() throws InterruptedException {
            super.initializeValidation();

            for (CountDownLatch prevTx : previousTransactions) {
                prevTx.await();
            }
        }

        @Override
        protected void finalizeValidation(Object result, boolean exception) {
            super.finalizeValidation(result, exception);
            if(prepareCommand.isOnePhaseCommit()) {
                finishTransaction(prepareCommand.getAffectedKeys(), barrier);
            }
        }
    }

    private class TxInfo {
        Set<Object> keys = new HashSet<Object>();
        CountDownLatch barrier;

        private TxInfo(Set<Object> keys, CountDownLatch barrier) {
            this.keys = keys;
            this.barrier = barrier;
        }
    }


    //TODO add description
    @ManagedAttribute
    @Metric
    public int getThreadPoolCoreSize() {
        return threadPoolExecutor.getCorePoolSize();
    }

    @ManagedAttribute
    @Metric
    public int getThreadPoolMaximumPoolSize() {
        return threadPoolExecutor.getMaximumPoolSize();
    }

    @ManagedAttribute
    @Metric
    public long getThreadPoolKeepTime() {
        return threadPoolExecutor.getKeepAliveTime(TimeUnit.MILLISECONDS);
    }

    @ManagedAttribute
    @Metric
    public int getNumberOfTransactionInPendingQueue() {
        return threadPoolExecutor.getQueue().size();
    }

    @ManagedAttribute
    @Metric
    public int getThreadPoolActiveThreads() {
        return threadPoolExecutor.getActiveCount();
    }

    @ManagedOperation
    @Operation
    public void setThreadPoolCoreSize(int size) {
        if(needsMultiThreadValidation) {
            threadPoolExecutor.setCorePoolSize(size);
        }
    }

    @ManagedOperation
    @Operation
    public void setThreadPoolMaximumPoolSize(int size) {
        if(needsMultiThreadValidation) {
            threadPoolExecutor.setMaximumPoolSize(size);
        }
    }

    @ManagedOperation
    @Operation
    public void setThreadPoolKeepTime(long time) {
        threadPoolExecutor.setKeepAliveTime(time, TimeUnit.MILLISECONDS);
    }
}
