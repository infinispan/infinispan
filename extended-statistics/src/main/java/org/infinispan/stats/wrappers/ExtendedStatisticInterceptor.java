package org.infinispan.stats.wrappers;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.infinispan.stats.container.ExtendedStatistic.*;
import static org.infinispan.stats.percentiles.PercentileStatistic.*;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Arrays;

import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.read.GetManyCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.tx.TransactionBoundaryCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.annotations.Start;
import org.infinispan.interceptors.base.BaseCustomInterceptor;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.jmx.annotations.Parameter;
import org.infinispan.remoting.RemoteException;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.stats.CacheStatisticManager;
import org.infinispan.stats.ExtendedStatisticNotFoundException;
import org.infinispan.stats.container.ExtendedStatistic;
import org.infinispan.stats.logging.Log;
import org.infinispan.transaction.WriteSkewException;
import org.infinispan.transaction.impl.TransactionTable;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.TimeService;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.concurrent.locks.DeadlockDetectedException;
import org.infinispan.util.concurrent.locks.LockManager;
import org.infinispan.util.logging.LogFactory;

/**
 * Take the statistics about relevant visitable commands.
 *
 * @author Roberto Palmieri
 * @author Sebastiano Peluso
 * @author Diego Didona
 * @author Pedro Ruivo
 * @since 6.0
 */
@MBean(objectName = "ExtendedStatistics", description = "Component that manages and exposes extended statistics " +
      "relevant to transactions.")
public class ExtendedStatisticInterceptor extends BaseCustomInterceptor {

   private static final Log log = LogFactory.getLog(ExtendedStatisticInterceptor.class, Log.class);
   private TransactionTable transactionTable;
   private RpcManager rpcManager;
   private DistributionManager distributionManager;
   private CacheStatisticManager cacheStatisticManager;
   private TimeService timeService;

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      return visitWriteCommand(ctx, command, command.getKey());
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      return visitWriteCommand(ctx, command, command.getKey());
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      return visitWriteCommand(ctx, command, command.getKey());
   }

   @Override
   public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
      if (log.isTraceEnabled()) {
         log.tracef("Visit Get Key Value command %s. Is it in transaction scope? %s. Is it local? %s", command,
                    ctx.isInTxScope(), ctx.isOriginLocal());
      }
      Object retVal;
      if (ctx.isInTxScope()) {
         long start = timeService.time();
         retVal = invokeNextInterceptor(ctx, command);
         long end = timeService.time();
         initStatsIfNecessary(ctx);
         if (isRemote(command.getKey())) {
            cacheStatisticManager.increment(NUM_REMOTE_GET, getGlobalTransaction(ctx), ctx.isOriginLocal());
            cacheStatisticManager.add(REMOTE_GET_EXECUTION, timeService.timeDuration(start, end, NANOSECONDS),
                                      getGlobalTransaction(ctx), ctx.isOriginLocal());
         }
         cacheStatisticManager.add(ALL_GET_EXECUTION, timeService.timeDuration(start, end, NANOSECONDS),
                                   getGlobalTransaction(ctx), ctx.isOriginLocal());
         cacheStatisticManager.increment(NUM_GET, getGlobalTransaction(ctx), ctx.isOriginLocal());
      } else {
         retVal = invokeNextInterceptor(ctx, command);
      }
      return retVal;
   }

   @Override
   public Object visitGetManyCommand(InvocationContext ctx, GetManyCommand command) throws Throwable {
      if (log.isTraceEnabled()) {
         log.tracef("Visit GetMany command %s. Is it in transaction scope? %s. Is it local? %s", command,
               ctx.isInTxScope(), ctx.isOriginLocal());
      }
      Object retVal;
      if (ctx.isInTxScope()) {
         long start = timeService.time();
         retVal = invokeNextInterceptor(ctx, command);
         long end = timeService.time();
         initStatsIfNecessary(ctx);
         int numRemote = 0;
         for (Object key : command.getKeys()) {
            if (isRemote(key)) numRemote++;
         }
         if (numRemote > 0) {
            cacheStatisticManager.add(NUM_REMOTE_GET, numRemote, getGlobalTransaction(ctx), ctx.isOriginLocal());
            cacheStatisticManager.add(REMOTE_GET_EXECUTION, timeService.timeDuration(start, end, NANOSECONDS),
                  getGlobalTransaction(ctx), ctx.isOriginLocal());
         }
         cacheStatisticManager.add(ALL_GET_EXECUTION, timeService.timeDuration(start, end, NANOSECONDS),
               getGlobalTransaction(ctx), ctx.isOriginLocal());
         cacheStatisticManager.add(NUM_GET, command.getKeys().size(), getGlobalTransaction(ctx), ctx.isOriginLocal());
      } else {
         retVal = invokeNextInterceptor(ctx, command);
      }
      return retVal;
   }

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      GlobalTransaction globalTransaction = command.getGlobalTransaction();
      if (log.isTraceEnabled()) {
         log.tracef("Visit Prepare command %s. Is it local?. Transaction is %s", command,
                    ctx.isOriginLocal(), globalTransaction.globalId());
      }
      initStatsIfNecessary(ctx);
      cacheStatisticManager.onPrepareCommand(globalTransaction, ctx.isOriginLocal());
      if (command.hasModifications()) {
         cacheStatisticManager.markAsWriteTransaction(globalTransaction, ctx.isOriginLocal());
      }

      boolean success = false;
      try {
         long start = timeService.time();
         Object ret = invokeNextInterceptor(ctx, command);
         long end = timeService.time();
         updateTime(PREPARE_EXECUTION_TIME, NUM_PREPARE_COMMAND, start, end, globalTransaction, ctx.isOriginLocal());
         success = true;
         return ret;
      } catch (TimeoutException e) {
         if (ctx.isOriginLocal() && isLockTimeout(e)) {
            cacheStatisticManager.increment(NUM_LOCK_FAILED_TIMEOUT, globalTransaction, ctx.isOriginLocal());
         }
         throw e;
      } catch (DeadlockDetectedException e) {
         if (ctx.isOriginLocal()) {
            cacheStatisticManager.increment(NUM_LOCK_FAILED_DEADLOCK, globalTransaction, ctx.isOriginLocal());
         }
         throw e;
      } catch (WriteSkewException e) {
         if (ctx.isOriginLocal()) {
            cacheStatisticManager.increment(NUM_WRITE_SKEW, globalTransaction, ctx.isOriginLocal());
         }
         throw e;
      } catch (RemoteException remote) {
         if (ctx.isOriginLocal()) {
            ExtendedStatistic stat = null;
            Throwable cause = remote.getCause();
            while (cause != null) {
               if (cause instanceof TimeoutException) {
                  stat = NUM_LOCK_FAILED_TIMEOUT;
                  break;
               } else if (cause instanceof DeadlockDetectedException) {
                  stat = NUM_LOCK_FAILED_DEADLOCK;
                  break;
               } else if (cause instanceof WriteSkewException) {
                  stat = NUM_WRITE_SKEW;
                  break;
               }
               cause = cause.getCause();
            }
            if (stat != null) {
               cacheStatisticManager.increment(stat, globalTransaction, true);
            }
         }
         throw remote;
      } finally {
         if (command.isOnePhaseCommit()) {
            boolean local = ctx.isOriginLocal();
            cacheStatisticManager.setTransactionOutcome(success, globalTransaction, ctx.isOriginLocal());
            cacheStatisticManager.terminateTransaction(globalTransaction, local, !local);
         }
      }
   }

   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      return visitSecondPhaseCommand(ctx, command, true, COMMIT_EXECUTION_TIME, NUM_COMMIT_COMMAND);
   }

   @Override
   public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
      return visitSecondPhaseCommand(ctx, command, false, ROLLBACK_EXECUTION_TIME, NUM_ROLLBACK_COMMAND);
   }

   @ManagedAttribute(description = "Average Prepare Round-Trip Time duration (in microseconds)",
                     displayName = "Average Prepare RTT")
   public double getAvgPrepareRtt() {
      return getAttribute(SYNC_PREPARE_TIME);
   }

   @ManagedAttribute(description = "Average Commit Round-Trip Time duration (in microseconds)",
                     displayName = "Average Commit RTT")
   public double getAvgCommitRtt() {
      return getAttribute(SYNC_COMMIT_TIME);
   }

   @ManagedAttribute(description = "Average Remote Get Round-Trip Time duration (in microseconds)",
                     displayName = "Average Remote Get RTT")
   public double getAvgRemoteGetRtt() {
      return getAttribute(SYNC_GET_TIME);
   }

   @ManagedAttribute(description = "Average Rollback Round-Trip Time duration (in microseconds)",
                     displayName = "Average Rollback RTT")
   public double getAvgRollbackRtt() {
      return getAttribute(SYNC_ROLLBACK_TIME);
   }

   @ManagedAttribute(description = "Average asynchronous Prepare duration (in microseconds)",
                     displayName = "Average Prepare Async")
   public double getAvgPrepareAsync() {
      return getAttribute(ASYNC_PREPARE_TIME);
   }

   @ManagedAttribute(description = "Average asynchronous Commit duration (in microseconds)",
                     displayName = "Average Commit Async")
   public double getAvgCommitAsync() {
      return getAttribute(ASYNC_COMMIT_TIME);
   }

   @ManagedAttribute(description = "Average asynchronous Complete Notification duration (in microseconds)",
                     displayName = "Average Complete Notification Async")
   public double getAvgCompleteNotificationAsync() {
      return getAttribute(ASYNC_COMPLETE_NOTIFY_TIME);
   }

   @ManagedAttribute(description = "Average asynchronous Rollback duration (in microseconds)",
                     displayName = "Average Rollback Async")
   public double getAvgRollbackAsync() {
      return getAttribute(ASYNC_ROLLBACK_TIME);
   }

   @ManagedAttribute(description = "Average number of nodes in Commit destination set",
                     displayName = "Average Number of Nodes in Commit Destination Set")
   public double getAvgNumNodesCommit() {
      return getAttribute(NUM_NODES_COMMIT);
   }

   @ManagedAttribute(description = "Average number of nodes in Complete Notification destination set",
                     displayName = "Average Number of Nodes in Complete Notification Destination Set")
   public double getAvgNumNodesCompleteNotification() {
      return getAttribute(NUM_NODES_COMPLETE_NOTIFY);
   }

   @ManagedAttribute(description = "Average number of nodes in Remote Get destination set",
                     displayName = "Average Number of Nodes in Remote Get Destination Set")
   public double getAvgNumNodesRemoteGet() {
      return getAttribute(NUM_NODES_GET);
   }

   @ManagedAttribute(description = "Average number of nodes in Prepare destination set",
                     displayName = "Average Number of Nodes in Prepare Destination Set")
   public double getAvgNumNodesPrepare() {
      return getAttribute(NUM_NODES_PREPARE);
   }

   //JMX exposed methods

   @ManagedAttribute(description = "Average number of nodes in Rollback destination set",
                     displayName = "Average Number of Nodes in Rollback Destination Set")
   public double getAvgNumNodesRollback() {
      return getAttribute(NUM_NODES_ROLLBACK);
   }

   @ManagedAttribute(description = "Local execution time of a transaction without the time waiting for lock acquisition",
                     displayName = "Local Execution Time Without Locking Time")
   public double getLocalExecutionTimeWithoutLock() {
      return getAttribute(LOCAL_EXEC_NO_CONT);
   }

   @ManagedAttribute(description = "Average lock holding time (in microseconds)",
                     displayName = "Average Lock Holding Time")
   public double getAvgLockHoldTime() {
      return getAttribute(LOCK_HOLD_TIME);
   }

   @ManagedAttribute(description = "Average lock local holding time (in microseconds)",
                     displayName = "Average Lock Local Holding Time")
   public double getAvgLocalLockHoldTime() {
      return getAttribute(LOCK_HOLD_TIME_LOCAL);
   }

   @ManagedAttribute(description = "Average lock remote holding time (in microseconds)",
                     displayName = "Average Lock Remote Holding Time")
   public double getAvgRemoteLockHoldTime() {
      return getAttribute(LOCK_HOLD_TIME_REMOTE);
   }

   @ManagedAttribute(description = "Average local commit duration time (2nd phase only) (in microseconds)",
                     displayName = "Average Commit Time")
   public double getAvgCommitTime() {
      return getAttribute(COMMIT_EXECUTION_TIME);
   }

   @ManagedAttribute(description = "Average local rollback duration time (2nd phase only) (in microseconds)",
                     displayName = "Average Rollback Time")
   public double getAvgRollbackTime() {
      return getAttribute(ROLLBACK_EXECUTION_TIME);
   }

   @ManagedAttribute(description = "Average prepare command size (in bytes)",
                     displayName = "Average Prepare Command Size")
   public double getAvgPrepareCommandSize() {
      return getAttribute(PREPARE_COMMAND_SIZE);
   }

   @ManagedAttribute(description = "Average commit command size (in bytes)",
                     displayName = "Average Commit Command Size")
   public double getAvgCommitCommandSize() {
      return getAttribute(COMMIT_COMMAND_SIZE);
   }

   @ManagedAttribute(description = "Average clustered get command size (in bytes)",
                     displayName = "Average Clustered Get Command Size")
   public double getAvgClusteredGetCommandSize() {
      return getAttribute(CLUSTERED_GET_COMMAND_SIZE);
   }

   @ManagedAttribute(description = "Average time waiting for the lock acquisition (in microseconds)",
                     displayName = "Average Lock Waiting Time")
   public double getAvgLockWaitingTime() {
      return getAttribute(LOCK_WAITING_TIME);
   }

   @ManagedAttribute(description = "Average transaction arrival rate, originated locally and remotely (in transaction " +
         "per second)",
                     displayName = "Average Transaction Arrival Rate")
   public double getAvgTxArrivalRate() {
      return getAttribute(ARRIVAL_RATE);
   }

   @ManagedAttribute(description = "Percentage of Write transaction executed locally (committed and aborted)",
                     displayName = "Percentage of Write Transactions")
   public double getPercentageWriteTransactions() {
      return getAttribute(WRITE_TX_PERCENTAGE);
   }

   @ManagedAttribute(description = "Percentage of Write transaction executed in all successfully executed " +
         "transactions (local transaction only)",
                     displayName = "Percentage of Successfully Write Transactions")
   public double getPercentageSuccessWriteTransactions() {
      return getAttribute(SUCCESSFUL_WRITE_TX_PERCENTAGE);
   }

   @ManagedAttribute(description = "The number of aborted transactions due to timeout in lock acquisition",
                     displayName = "Number of Aborted Transaction due to Lock Acquisition Timeout")
   public double getNumAbortedTxDueTimeout() {
      return getAttribute(NUM_LOCK_FAILED_TIMEOUT);
   }

   @ManagedAttribute(description = "The number of aborted transactions due to deadlock",
                     displayName = "Number of Aborted Transaction due to Deadlock")
   public double getNumAbortedTxDueDeadlock() {
      return getAttribute(NUM_LOCK_FAILED_DEADLOCK);
   }

   @ManagedAttribute(description = "Average successful read-only transaction duration (in microseconds)",
                     displayName = "Average Read-Only Transaction Duration")
   public double getAvgReadOnlyTxDuration() {
      return getAttribute(RO_TX_SUCCESSFUL_EXECUTION_TIME);
   }

   @ManagedAttribute(description = "Average successful write transaction duration (in microseconds)",
                     displayName = "Average Write Transaction Duration")
   public double getAvgWriteTxDuration() {
      return getAttribute(WR_TX_SUCCESSFUL_EXECUTION_TIME);
   }

   @ManagedAttribute(description = "Average aborted write transaction duration (in microseconds)",
                     displayName = "Average Aborted Write Transaction Duration")
   public double getAvgAbortedWriteTxDuration() {
      return getAttribute(WR_TX_ABORTED_EXECUTION_TIME);
   }

   @ManagedAttribute(description = "Average number of locks per write local transaction",
                     displayName = "Average Number of Lock per Local Transaction")
   public double getAvgNumOfLockLocalTx() {
      return getAttribute(NUM_LOCK_PER_LOCAL_TX);
   }

   @ManagedAttribute(description = "Average number of locks per write remote transaction",
                     displayName = "Average Number of Lock per Remote Transaction")
   public double getAvgNumOfLockRemoteTx() {
      return getAttribute(NUM_LOCK_PER_REMOTE_TX);
   }

   @ManagedAttribute(description = "Average number of locks per successfully write local transaction",
                     displayName = "Average Number of Lock per Successfully Local Transaction")
   public double getAvgNumOfLockSuccessLocalTx() {
      return getAttribute(NUM_HELD_LOCKS_SUCCESS_LOCAL_TX);
   }

   @ManagedAttribute(description = "Average time it takes to execute the prepare command locally (in microseconds)",
                     displayName = "Average Local Prepare Execution Time")
   public double getAvgLocalPrepareTime() {
      return getAttribute(LOCAL_PREPARE_EXECUTION_TIME);
   }

   @ManagedAttribute(description = "Average time it takes to execute the prepare command remotely (in microseconds)",
                     displayName = "Average Remote Prepare Execution Time")
   public double getAvgRemotePrepareTime() {
      return getAttribute(REMOTE_PREPARE_EXECUTION_TIME);
   }

   @ManagedAttribute(description = "Average time it takes to execute the commit command locally (in microseconds)",
                     displayName = "Average Local Commit Execution Time")
   public double getAvgLocalCommitTime() {
      return getAttribute(LOCAL_COMMIT_EXECUTION_TIME);
   }

   @ManagedAttribute(description = "Average time it takes to execute the commit command remotely (in microseconds)",
                     displayName = "Average Remote Commit Execution Time")
   public double getAvgRemoteCommitTime() {
      return getAttribute(REMOTE_COMMIT_EXECUTION_TIME);
   }

   @ManagedAttribute(description = "Average time it takes to execute the rollback command locally (in microseconds)",
                     displayName = "Average Local Rollback Execution Time")
   public double getAvgLocalRollbackTime() {
      return getAttribute(LOCAL_ROLLBACK_EXECUTION_TIME);
   }

   @ManagedAttribute(description = "Average time it takes to execute the rollback command remotely (in microseconds)",
                     displayName = "Average Remote Rollback Execution Time")
   public double getAvgRemoteRollbackTime() {
      return getAttribute(REMOTE_ROLLBACK_EXECUTION_TIME);
   }

   @ManagedAttribute(description = "Abort Rate",
                     displayName = "Abort Rate")
   public double getAbortRate() {
      return getAttribute(ABORT_RATE);
   }

   @ManagedAttribute(description = "Throughput (in transactions per second)",
                     displayName = "Throughput")
   public double getThroughput() {
      return getAttribute(THROUGHPUT);
   }

   @ManagedAttribute(description = "Average number of get operations per (local) read-only transaction",
                     displayName = "Average number of get operations per (local) read-only transaction")
   public double getAvgGetsPerROTransaction() {
      return getAttribute(NUM_GETS_RO_TX);
   }

   @ManagedAttribute(description = "Average number of get operations per (local) read-write transaction",
                     displayName = "Average number of get operations per (local) read-write transaction")
   public double getAvgGetsPerWrTransaction() {
      return getAttribute(NUM_GETS_WR_TX);
   }

   @ManagedAttribute(description = "Average number of remote get operations per (local) read-write transaction",
                     displayName = "Average number of remote get operations per (local) read-write transaction")
   public double getAvgRemoteGetsPerWrTransaction() {
      return getAttribute(NUM_REMOTE_GETS_WR_TX);
   }

   @ManagedAttribute(description = "Average number of remote get operations per (local) read-only transaction",
                     displayName = "Average number of remote get operations per (local) read-only transaction")
   public double getAvgRemoteGetsPerROTransaction() {
      return getAttribute(NUM_REMOTE_GETS_RO_TX);
   }

   @ManagedAttribute(description = "Average cost of a remote get",
                     displayName = "Remote get cost")
   public double getRemoteGetExecutionTime() {
      return getAttribute(REMOTE_GET_EXECUTION);
   }

   @ManagedAttribute(description = "Average number of put operations per (local) read-write transaction",
                     displayName = "Average number of put operations per (local) read-write transaction")
   public double getAvgPutsPerWrTransaction() {
      return getAttribute(NUM_PUTS_WR_TX);
   }

   @ManagedAttribute(description = "Average number of remote put operations per (local) read-write transaction",
                     displayName = "Average number of remote put operations per (local) read-write transaction")
   public double getAvgRemotePutsPerWrTransaction() {
      return getAttribute(NUM_REMOTE_PUTS_WR_TX);
   }

   @ManagedAttribute(description = "Average cost of a remote put",
                     displayName = "Remote put cost")
   public double getRemotePutExecutionTime() {
      return getAttribute(REMOTE_PUT_EXECUTION);
   }

   @ManagedAttribute(description = "Number of gets performed since last reset",
                     displayName = "Number of Gets")
   public double getNumberOfGets() {
      return getAttribute(NUM_GET);
   }

   @ManagedAttribute(description = "Number of remote gets performed since last reset",
                     displayName = "Number of Remote Gets")
   public double getNumberOfRemoteGets() {
      return getAttribute(NUM_REMOTE_GET);
   }

   @ManagedAttribute(description = "Number of puts performed since last reset",
                     displayName = "Number of Puts")
   public double getNumberOfPuts() {
      return getAttribute(NUM_PUT);
   }

   @ManagedAttribute(description = "Number of remote puts performed since last reset",
                     displayName = "Number of Remote Puts")
   public double getNumberOfRemotePuts() {
      return getAttribute(NUM_REMOTE_PUT);
   }

   @ManagedAttribute(description = "Number of committed transactions since last reset",
                     displayName = "Number Of Commits")
   public double getNumberOfCommits() {
      return getAttribute(NUM_COMMITTED_TX);
   }

   @ManagedAttribute(description = "Number of local committed transactions since last reset",
                     displayName = "Number Of Local Commits")
   public double getNumberOfLocalCommits() {
      return getAttribute(NUM_LOCAL_COMMITTED_TX);
   }

   @ManagedAttribute(description = "Write skew probability",
                     displayName = "Write Skew Probability")
   public double getWriteSkewProbability() {
      return getAttribute(WRITE_SKEW_PROBABILITY);
   }

   @ManagedOperation(description = "K-th percentile of local read-only transactions execution time",
                     displayName = "K-th Percentile Local Read-Only Transactions")
   public double getPercentileLocalReadOnlyTransaction(@Parameter(name = "percentile") int percentile) {
      return cacheStatisticManager.getPercentile(RO_LOCAL_EXECUTION, percentile);
   }

   @ManagedOperation(description = "K-th percentile of remote read-only transactions execution time",
                     displayName = "K-th Percentile Remote Read-Only Transactions")
   public double getPercentileRemoteReadOnlyTransaction(@Parameter(name = "percentile") int percentile) {
      return cacheStatisticManager.getPercentile(RO_REMOTE_EXECUTION, percentile);
   }

   @ManagedOperation(description = "K-th percentile of local write transactions execution time",
                     displayName = "K-th Percentile Local Write Transactions")
   public double getPercentileLocalRWriteTransaction(@Parameter(name = "percentile") int percentile) {
      return cacheStatisticManager.getPercentile(WR_LOCAL_EXECUTION, percentile);
   }

   @ManagedOperation(description = "K-th percentile of remote write transactions execution time",
                     displayName = "K-th Percentile Remote Write Transactions")
   public double getPercentileRemoteWriteTransaction(@Parameter(name = "percentile") int percentile) {
      return cacheStatisticManager.getPercentile(WR_REMOTE_EXECUTION, percentile);
   }

   @ManagedOperation(description = "Reset all the statistics collected",
                     displayName = "Reset All Statistics")
   public void resetStatistics() {
      cacheStatisticManager.reset();
   }

   @ManagedAttribute(description = "Average Local processing Get time (in microseconds)",
                     displayName = "Average Local Get time")
   public double getAvgLocalGetTime() {
      return getAttribute(LOCAL_GET_EXECUTION);
   }

   @ManagedAttribute(description = "Number of nodes in the cluster",
                     displayName = "Number of nodes")
   public double getNumNodes() {
      if (rpcManager == null) {
         return 1; //local mode
      }
      return rpcManager.getTransport().getMembers().size();
   }

   @ManagedAttribute(description = "Number of replicas for each key",
                     displayName = "Replication Degree")
   public double getReplicationDegree() {
      if (distributionManager != null) {
         //distributed mode
         return distributionManager.getConsistentHash().getNumOwners();
      } else if (rpcManager != null) {
         //replicated or other clustered mode
         return this.rpcManager.getTransport().getMembers().size();
      }
      //local mode
      return 1;
   }

   @ManagedAttribute(description = "Number of concurrent transactions executing on the current node",
                     displayName = "Local Active Transactions")
   public double getLocalActiveTransactions() {
      if (transactionTable != null) {
         return transactionTable.getLocalTxCount();
      }
      return 0;
   }

   @ManagedAttribute(description = "Average Response Time",
                     displayName = "Average Response Time")
   public double getAvgResponseTime() {
      return getAttribute(RESPONSE_TIME);
   }

   @ManagedOperation(description = "Returns the raw value for the statistic",
                     displayName = "Get Statistic Value")
   public final double getStatisticValue(@Parameter(description = "Statistic name") String statName) {
      if (statName == null) {
         return 0;
      }
      for (ExtendedStatistic statistic : ExtendedStatistic.values()) {
         if (statistic.name().equalsIgnoreCase(statName)) {
            return getAttribute(statistic);
         }
      }
      return 0;
   }

   @ManagedAttribute(description = "Returns all the available statistics",
                     displayName = "Available Statistics")
   public final String getAvailableExtendedStatistics() {
      return Arrays.toString(ExtendedStatistic.values());
   }

   @ManagedOperation(description = "Dumps the current cache statistic values",
                     displayName = "Dump Cache Statistics")
   public final String dumpStatistics() {
      return cacheStatisticManager.dumpCacheStatistics();
   }

   @ManagedOperation(description = "Dumps the current cache statistic values to System.out",
                     displayName = "Dump Cache Statistics to System.out")
   public final void dumpStatisticsToSystemOut() {
      cacheStatisticManager.dumpCacheStatisticsTo(System.out);
   }

   @ManagedOperation(description = "Dumps the current cache statistic values to a file",
                     displayName = "Dump cache Statistics to file")
   public final void dumpStatisticToFile(@Parameter(description = "The file path") String filePath) throws IOException {
      PrintStream stream = null;
      try {
         stream = new PrintStream(new File(filePath));
         cacheStatisticManager.dumpCacheStatisticsTo(stream);
      } finally {
         if (stream != null) {
            stream.close();
         }
      }
   }

   public final CacheStatisticManager getCacheStatisticManager() {
      return cacheStatisticManager;
   }

   //public to be used by the tests
   public double getAttribute(ExtendedStatistic statistic) {
      try {
         return cacheStatisticManager.getAttribute(statistic);
      } catch (ExtendedStatisticNotFoundException e) {
         log.unableToGetStatistic(statistic, e);
      }
      return 0;
   }

   @Override
   @Start(priority = 11) //it is going to replace LockManager and RpcManager. set to 11 because DeadlockDetectingLockManager has default priority of 10 and RpcManagerImpl has priority 9.
   protected void start() {
      super.start();
      log.startExtendedStatisticInterceptor();
      this.timeService = cache.getAdvancedCache().getComponentRegistry().getTimeService();
      this.cacheStatisticManager = new CacheStatisticManager(cacheConfiguration, timeService);
      this.transactionTable = cache.getAdvancedCache().getComponentRegistry().getComponent(TransactionTable.class);
      this.distributionManager = cache.getAdvancedCache().getDistributionManager();
      replace();
   }

   private Object visitSecondPhaseCommand(TxInvocationContext ctx, TransactionBoundaryCommand command, boolean commit,
                                          ExtendedStatistic duration, ExtendedStatistic counter) throws Throwable {
      GlobalTransaction globalTransaction = command.getGlobalTransaction();
      if (log.isTraceEnabled()) {
         log.tracef("Visit 2nd phase command %s. Is it local? %s. Transaction is %s", command,
                    ctx.isOriginLocal(), globalTransaction.globalId());
      }
      long start = timeService.time();
      Object ret = invokeNextInterceptor(ctx, command);
      long end = timeService.time();
      updateTime(duration, counter, start, end, globalTransaction, ctx.isOriginLocal());
      cacheStatisticManager.setTransactionOutcome(commit, globalTransaction, ctx.isOriginLocal());
      cacheStatisticManager.terminateTransaction(globalTransaction, true, true);
      return ret;
   }

   private Object visitWriteCommand(InvocationContext ctx, WriteCommand command, Object key) throws Throwable {
      if (log.isTraceEnabled()) {
         log.tracef("Visit write command %s. Is it in transaction scope? %s. Is it local? %s", command,
                    ctx.isInTxScope(), ctx.isOriginLocal());
      }
      Object ret;
      if (ctx.isInTxScope()) {
         long start = timeService.time();
         long end;
         try {
            ret = invokeNextInterceptor(ctx, command);
         } catch (TimeoutException e) {
            if (ctx.isOriginLocal() && isLockTimeout(e)) {
               initStatsIfNecessary(ctx);
               cacheStatisticManager.increment(NUM_LOCK_FAILED_TIMEOUT, getGlobalTransaction(ctx), ctx.isOriginLocal());
            }
            throw e;
         } catch (DeadlockDetectedException e) {
            if (ctx.isOriginLocal()) {
               initStatsIfNecessary(ctx);
               cacheStatisticManager.increment(NUM_LOCK_FAILED_DEADLOCK, getGlobalTransaction(ctx), ctx.isOriginLocal());
            }
            throw e;
         } catch (WriteSkewException e) {
            if (ctx.isOriginLocal()) {
               initStatsIfNecessary(ctx);
               cacheStatisticManager.increment(NUM_WRITE_SKEW, getGlobalTransaction(ctx), ctx.isOriginLocal());
            }
            throw e;
         } catch (RemoteException remote) {
            if (ctx.isOriginLocal()) {
               ExtendedStatistic stat = null;
               Throwable cause = remote.getCause();
               while (cause != null) {
                  if (cause instanceof TimeoutException) {
                     stat = NUM_LOCK_FAILED_TIMEOUT;
                     break;
                  } else if (cause instanceof DeadlockDetectedException) {
                     stat = NUM_LOCK_FAILED_DEADLOCK;
                     break;
                  } else if (cause instanceof WriteSkewException) {
                     stat = NUM_WRITE_SKEW;
                     break;
                  }
                  cause = cause.getCause();
               }
               if (stat != null) {
                  initStatsIfNecessary(ctx);
                  cacheStatisticManager.increment(stat, getGlobalTransaction(ctx), true);
               }
            }
            throw remote;
         } finally {
            end = timeService.time();
            initStatsIfNecessary(ctx);
            cacheStatisticManager.increment(NUM_PUT, getGlobalTransaction(ctx), ctx.isOriginLocal());
            cacheStatisticManager.markAsWriteTransaction(getGlobalTransaction(ctx), ctx.isOriginLocal());
         }
         if (isRemote(key)) {
            cacheStatisticManager.add(REMOTE_PUT_EXECUTION, timeService.timeDuration(start, end, NANOSECONDS),
                                      getGlobalTransaction(ctx), ctx.isOriginLocal());
            cacheStatisticManager.increment(NUM_REMOTE_PUT, getGlobalTransaction(ctx), ctx.isOriginLocal());
         }
         return ret;
      } else
         return invokeNextInterceptor(ctx, command);
   }

   private GlobalTransaction getGlobalTransaction(InvocationContext context) {
      if (context.isInTxScope()) {
         return ((TxInvocationContext) context).getGlobalTransaction();
      }
      return null;
   }

   private boolean isRemote(Object key) {
      return distributionManager != null && !distributionManager.getLocality(key).isLocal();
   }

   private void replace() {
      log.replaceComponents();
      ComponentRegistry componentRegistry = cache.getAdvancedCache().getComponentRegistry();

      replaceRpcManager(componentRegistry);
      replaceLockManager(componentRegistry);
      componentRegistry.rewire();
   }

   private void replaceLockManager(ComponentRegistry componentRegistry) {
      LockManager oldLockManager = componentRegistry.getComponent(LockManager.class);
      LockManager newLockManager = new ExtendedStatisticLockManager(oldLockManager, cacheStatisticManager, timeService);
      log.replaceComponent("LockManager", oldLockManager, newLockManager);
      componentRegistry.registerComponent(newLockManager, LockManager.class);
   }

   private void replaceRpcManager(ComponentRegistry componentRegistry) {
      RpcManager oldRpcManager = componentRegistry.getComponent(RpcManager.class);
      if (oldRpcManager == null) {
         //local mode
         return;
      }
      RpcManager newRpcManager = new ExtendedStatisticRpcManager(oldRpcManager, cacheStatisticManager, timeService);
      log.replaceComponent("RpcManager", oldRpcManager, newRpcManager);
      componentRegistry.registerComponent(newRpcManager, RpcManager.class);
      this.rpcManager = newRpcManager;
   }

   private void initStatsIfNecessary(InvocationContext ctx) {
      if (ctx.isInTxScope())
         cacheStatisticManager.beginTransaction(getGlobalTransaction(ctx), ctx.isOriginLocal());
   }

   private boolean isLockTimeout(TimeoutException e) {
      return e.getMessage().startsWith("ISPN000299: Unable to acquire lock after");
   }

   private void updateTime(ExtendedStatistic duration, ExtendedStatistic counter, long initTime, long endTime,
                           GlobalTransaction globalTransaction, boolean local) {
      cacheStatisticManager.add(duration, timeService.timeDuration(initTime, endTime, NANOSECONDS), globalTransaction, local);
      cacheStatisticManager.increment(counter, globalTransaction, local);
   }
}
