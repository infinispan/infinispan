package org.infinispan.totalorder;

import org.infinispan.CacheException;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.versioning.EntryVersionsMap;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.transaction.LocalTransaction;
import org.infinispan.transaction.TransactionTable;
import org.infinispan.transaction.TxDependencyLatch;
import org.infinispan.transaction.totalOrder.TotalOrderRemoteTransaction;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.concurrent.IsolationLevel;
import org.infinispan.util.concurrent.WithinThreadExecutor;
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

import static org.infinispan.factories.KnownComponentNames.TOTAL_ORDER_EXECUTOR;
import static org.infinispan.util.Util.prettyPrintGlobalTransaction;

/**
 * this class is responsible to validate transactions in the total order based protocol. It ensures the delivered order
 * and will validate multiple transactions in parallel if they are non conflicting transaction.
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
@MBean(objectName = "TotalOrderManager", description = "Total order management")
public class TotalOrderManager {

   private static final Log log = LogFactory.getLog(TotalOrderManager.class);

   //map between GlobalTransaction and LocalTransaction. used to sync the threads in remote validation and the
   //transaction execution thread
   private final ConcurrentMap<GlobalTransaction, LocalTransaction> localTransactionMap =
         new ConcurrentHashMap<GlobalTransaction, LocalTransaction>();

   //this map is used to keep track of concurrent transactions
   private final ConcurrentMap<Object, TxDependencyLatch> keysLocked = new ConcurrentHashMap<Object, TxDependencyLatch>();

   private Configuration configuration;
   private InvocationContextContainer invocationContextContainer;
   private TransactionTable transactionTable;

   //the multithread validation is only possible in repeatable read with write skew, where we can validate non
   //conflicting transactions in parallel
   private volatile boolean needsMultiThreadValidation;
   private volatile ExecutorService validationExecutorService;

   private boolean trace;
   private boolean info;

   //some profiling information (wasted time in queue and validation duration)
   private final AtomicLong waitTimeInQueue = new AtomicLong(0);
   private final AtomicLong validationDuration = new AtomicLong(0);
   private final AtomicInteger numberOfTxValidated = new AtomicInteger(0);
   private final AtomicLong initializationDuration = new AtomicLong(0);
   private volatile boolean statisticsEnabled;

   @Inject
   public void inject(Configuration configuration, InvocationContextContainer invocationContextContainer,
                      TransactionTable transactionTable, @ComponentName(TOTAL_ORDER_EXECUTOR) ExecutorService e) {
      this.configuration = configuration;
      this.invocationContextContainer = invocationContextContainer;
      this.transactionTable = transactionTable;

      needsMultiThreadValidation = configuration.locking().isolationLevel() == IsolationLevel.REPEATABLE_READ &&
            configuration.locking().writeSkewCheck() && !configuration.transaction().use1PCInTotalOrder();

      if (needsMultiThreadValidation) {
         validationExecutorService = e;
      } else {
         validationExecutorService = new WithinThreadExecutor();
      }
   }

   @Start
   public void start() {
      setLogLevel();
      setStatisticsEnabled(configuration.jmxStatistics().enabled());

      if(info) {
         if (validationExecutorService instanceof ThreadPoolExecutor) {
            ThreadPoolExecutor tpe = (ThreadPoolExecutor) validationExecutorService;
            log.startTotalOrderManager(needsMultiThreadValidation ? "yes" : "no",
                  tpe.getCorePoolSize(),
                  tpe.getMaximumPoolSize(),
                  tpe.getKeepAliveTime(TimeUnit.MILLISECONDS));
         } else {
            log.startTotalOrderManager(needsMultiThreadValidation ? "yes" : "no");
         }
      }
   }

   @Stop
   public void stop() {
      localTransactionMap.clear();
      keysLocked.clear();
   }

   /**
    * set the boolean trace and info
    */
   private void setLogLevel() {
      trace = log.isTraceEnabled();
      info = log.isInfoEnabled();
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

      assert !ctx.isOriginLocal();

      Runnable r;
      if(needsMultiThreadValidation) {
         TotalOrderRemoteTransaction remoteTransaction = (TotalOrderRemoteTransaction) ctx.getCacheTransaction();
         MultiThreadValidation mtv = new MultiThreadValidation(prepareCommand, ctx, invoker, remoteTransaction);
         Set<TxDependencyLatch> previousTxs = new HashSet<TxDependencyLatch>();

         //this will collect all the count down latch corresponding to the previous transactions in the queue
         for(Object key : remoteTransaction.getModifiedKeys()) {
            TxDependencyLatch prevTx = keysLocked.put(key, remoteTransaction.getLatch());
            if(prevTx != null) {
               previousTxs.add(prevTx);
            }
         }

         mtv.setPreviousTransactions(previousTxs);
         r = mtv;

         if(trace) {
            log.tracef("Transaction [%s] write set is %s", remoteTransaction.getLatch(),
                  remoteTransaction.getModifiedKeys());
         }

      } else {
         r = new SingleThreadValidation(prepareCommand, ctx, invoker);
      }
      validationExecutorService.execute(r);
   }

   /**
    * this will mark a global transaction as finished. It will be invoked in the processing of the commit command
    * in repeatable read with write skew (not implemented yet!)
    * @param gtx the global transaction
    * @param ignoreNullTxInfo ignore null remote tx info
    */
   public void finishTransaction(GlobalTransaction gtx, boolean ignoreNullTxInfo) {
      if(trace) {
         log.tracef("transaction %s is finished", prettyPrintGlobalTransaction(gtx));
      }

      TotalOrderRemoteTransaction remoteTransaction = (TotalOrderRemoteTransaction) transactionTable.removeRemoteTransaction(gtx);
      if(remoteTransaction != null) {
         finishTransaction(remoteTransaction);
      } else if (!ignoreNullTxInfo) {
         log.remoteTransactionIsNull(prettyPrintGlobalTransaction(gtx));
      }
   }

   /**
    * this ensures the order between the commit/rollback commands and the prepare command.
    *
    * However, if the commit/rollback command is deliver first, then they don't need to wait until the prepare is deliver.
    *   The mark the remote transaction for commit or rollback and when the prepare arrives, it adapts its behaviour:
    *   -> if it must rollback, the prepare is discarded (no needing for processing)
    *   -> if it must commit, then it sets the one phase flag and wait for this turn, committing the modifications and
    *      it skips the write skew check (note: the commit command saves the new versions in remote transaction)
    *
    * If the prepare is already in process, then the commit/rollback is blocked until the validation is finished.
    *
    * @param remoteTransaction the transaction
    * @param commit true if it is a commit command, false if it is a rollback command
    * @param newVersions the new versions
    * @return true if the command needs to be processed, false otherwise
    */
   public boolean waitForTxPrepared(TotalOrderRemoteTransaction remoteTransaction, boolean commit,
                                    EntryVersionsMap newVersions) {
      GlobalTransaction gtx = remoteTransaction.getGlobalTransaction();
      if(trace) {
         log.tracef("Waiting until transaction %s is prepared. New versions are %s",
               prettyPrintGlobalTransaction(gtx), newVersions);
      }

      boolean needsToProcessCommand = false;
      try {
         needsToProcessCommand = remoteTransaction.waitPrepared(commit, newVersions);
      } catch (InterruptedException e) {
         log.timeoutWaitingUntilTransactionPrepared(prettyPrintGlobalTransaction(gtx));
         needsToProcessCommand = false;
      } finally {
         if(trace) {
            log.tracef("Transaction %s finishes the waiting time", prettyPrintGlobalTransaction(gtx));
         }
      }
      return needsToProcessCommand;
   }

   /**
    * remove the keys from the map (if their didn't change) and release the count down latch, unblocking
    * the next transaction
    *
    * @param remoteTransaction the transaction
    */
   public void finishTransaction(TotalOrderRemoteTransaction remoteTransaction) {
      TxDependencyLatch latch = remoteTransaction.getLatch();
      if (trace) {
         log.tracef("Releasing resources for transaction %s", remoteTransaction);
      }

      latch.countDown();
      for(Object key : remoteTransaction.getModifiedKeys()) {
         this.keysLocked.remove(key, latch);
      }
   }

   /**
    * updates the accumulating time for profiling information
    * @param creationTime the arrival timestamp of the prepare command to this component in remote
    * @param validationStartTime the processing start timestamp
    * @param validationEndTime the validation ending timestamp
    * @param initializationEndTime the initialization ending timestamp
    */
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
    * this class is used to validate transaction in read committed or repeatable read without write skew or when
    * the 1PC is set for total order.
    */
   private class SingleThreadValidation implements Runnable {

      protected final PrepareCommand prepareCommand;
      protected final TxInvocationContext txInvocationContext;
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

      //the set of others transaction's count down latch (it will be unblocked when the transaction finishes)
      private final Set<TxDependencyLatch> previousTransactions;

      private TotalOrderRemoteTransaction remoteTransaction = null;

      private MultiThreadValidation(PrepareCommand prepareCommand, TxInvocationContext txInvocationContext,
                                    CommandInterceptor invoker, TotalOrderRemoteTransaction remoteTransaction) {
         super(prepareCommand, txInvocationContext, invoker);
         this.previousTransactions = new HashSet<TxDependencyLatch>();
         this.remoteTransaction = remoteTransaction;
      }

      public void setPreviousTransactions(Set<TxDependencyLatch> previousTransactions) {
         this.previousTransactions.addAll(previousTransactions);
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

         if(remoteTransaction.isMarkedForRollback()) {
            throw new CacheException("Cannot prepare transaction" + gtx +". it was already marked as rollback");
         }

         if (previousTransactions.contains(remoteTransaction.getLatch())) {
            throw new IllegalStateException("Dependency transaction must not contains myself in the set");
         }

         for (TxDependencyLatch prevTx : previousTransactions) {
            if(trace) {
               log.tracef("Transaction %s will wait for %s", gtx, prevTx);
            }
            prevTx.await();
         }

         remoteTransaction.markForPreparing();

         if(remoteTransaction.isMarkedForRollback()) {
            throw new CacheException("Cannot prepare transaction" + gtx + ". it was already marked as rollback");
         }

         if (remoteTransaction.isMarkedForCommit()) {
            txInvocationContext.setFlags(Flag.SKIP_WRITE_SKEW_CHECK);
            prepareCommand.setOnePhaseCommit(true);
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
         remoteTransaction.markPreparedAndNotify();
         super.finalizeValidation(result, exception);
         if(prepareCommand.isOnePhaseCommit() || exception) {
            markTxCompleted();
         }
      }

      private void markTxCompleted() {
         finishTransaction(remoteTransaction);
         transactionTable.removeRemoteTransaction(prepareCommand.getGlobalTransaction());
      }
   }


   @ManagedAttribute(description = "The minimum number of threads in the thread pool")
   @Metric(displayName = "Minimum Number of Threads", displayType = DisplayType.DETAIL)
   public int getThreadPoolCoreSize() {
      if (validationExecutorService instanceof ThreadPoolExecutor) {
         return ((ThreadPoolExecutor) validationExecutorService).getCorePoolSize();
      } else {
         return -1;
      }
   }

   @ManagedAttribute(description = "The maximum number of threads in the thread pool")
   @Metric(displayName = "Maximum Number of Threads", displayType = DisplayType.DETAIL)
   public int getThreadPoolMaximumPoolSize() {
      if (validationExecutorService instanceof ThreadPoolExecutor) {
         return ((ThreadPoolExecutor) validationExecutorService).getMaximumPoolSize();
      } else {
         return -1;
      }
   }

   @ManagedAttribute(description = "The keep alive time of an idle thread in the thread pool (milliseconds)")
   @Metric(displayName = "Keep Alive Time of a Idle Thread", units = Units.MILLISECONDS,
         displayType = DisplayType.DETAIL)
   public long getThreadPoolKeepTime() {
      if (validationExecutorService instanceof ThreadPoolExecutor) {
         return ((ThreadPoolExecutor) validationExecutorService).getKeepAliveTime(TimeUnit.MILLISECONDS);
      } else {
         return -1;
      }
   }

   @ManagedAttribute(description = "The percentage of occupation of the queue")
   @Metric(displayName = "Percentage of Occupation of the Queue", units = Units.PERCENTAGE,
         displayType = DisplayType.SUMMARY)
   public double getNumberOfTransactionInPendingQueue() {
      if (validationExecutorService instanceof ThreadPoolExecutor) {
         BlockingQueue queue = ((ThreadPoolExecutor) validationExecutorService).getQueue();
         int remainingCapacity = queue.remainingCapacity();
         int actualSize = queue.size();

         double percentage;
         if ((Integer.MAX_VALUE - remainingCapacity) > actualSize) {
            percentage = actualSize * 100.0 / (remainingCapacity + actualSize);
         } else {
            percentage = actualSize * 100.0 / remainingCapacity;
         }

         return percentage > 100 ? 100.0 : percentage;
      } else {
         return -1D;
      }
   }

   @ManagedAttribute(description = "The approximate percentage of active threads in the thread pool")
   @Metric(displayName = "Percentage of Active Threads", units = Units.PERCENTAGE, displayType = DisplayType.SUMMARY)
   public double getPercentageActiveThreads() {
      if (validationExecutorService instanceof ThreadPoolExecutor) {
         int max = ((ThreadPoolExecutor) validationExecutorService).getMaximumPoolSize();
         int actual = ((ThreadPoolExecutor) validationExecutorService).getActiveCount();
         double percentage = actual * 100.0 / max;
         return percentage > 100 ? 100.0 : percentage;
      } else {
         return -1D;
      }
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
      if (validationExecutorService instanceof ThreadPoolExecutor) {
         ((ThreadPoolExecutor) validationExecutorService).setCorePoolSize(size);
      }
   }

   @ManagedOperation(description = "Set the maximum number of threads in the thread pool")
   @Operation(displayName = "Set Maximum Number Of Threads")
   public void setThreadPoolMaximumPoolSize(int size) {
      if (validationExecutorService instanceof ThreadPoolExecutor) {
         ((ThreadPoolExecutor) validationExecutorService).setMaximumPoolSize(size);
      }
   }

   @ManagedOperation(description = "Set the idle time of a thread in the thread pool (milliseconds)")
   @Operation(displayName = "Set Keep Alive Time of Idle Threads")
   public void setThreadPoolKeepTime(long time) {
      if (validationExecutorService instanceof ThreadPoolExecutor) {
         ((ThreadPoolExecutor) validationExecutorService).setKeepAliveTime(time, TimeUnit.MILLISECONDS);
      }
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
