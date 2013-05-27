package org.infinispan.stats.container;

/**
 * The available extended statistics
 *
 * @author Roberto Palmieri
 * @author Sebastiano Peluso
 * @author Diego Didona
 * @author Pedro Ruivo
 * @since 6.0
 */

public enum ExtendedStatistic {

   //Write transaction related statistics
   NUM_COMMITTED_WR_TX(true, true), //number of committed write transactions
   NUM_ABORTED_WR_TX(true, true), //number of aborted write transaction
   WR_TX_ABORTED_EXECUTION_TIME(true, true), //write tx execution time include the prepare and commit phase for aborted transactions
   WR_TX_SUCCESSFUL_EXECUTION_TIME(true, true), //write tx execution time including the prepare and commit phase for committed transactions
   LOCAL_EXEC_NO_CONT(true, false), //write tx execution time excluding the waiting time acquiring locks (pessimistic mode only)

   //Read-only transaction related statistics
   NUM_COMMITTED_RO_TX(true, true), // number of committed read-only transaction
   NUM_ABORTED_RO_TX(true, true), // number of aborted read-only transaction
   RO_TX_SUCCESSFUL_EXECUTION_TIME(true, true), // read-only tx execution time for aborted transaction
   RO_TX_ABORTED_EXECUTION_TIME(true, true), //read-only tx execution time for aborted transaction

   //Write transaction access
   NUM_PUTS_WR_TX(true, false), // number of puts on local keys per write transaction
   NUM_REMOTE_PUTS_WR_TX(true, false), // number of puts on remote keys per write transaction
   NUM_GETS_WR_TX(true, false), //number of gets on local keys per write transaction
   NUM_REMOTE_GETS_WR_TX(true, false), //number of gets on remote keys per write transaction

   //Read-Only transaction access
   NUM_GETS_RO_TX(true, false), //number of gets on local keys per read-only transaction
   NUM_REMOTE_GETS_RO_TX(true, false), //number of gets on remote keys per read-only transaction

   //Accesses (used during runtime. when the transaction ends, they are copied to the access above)
   NUM_REMOTE_GET(true, false), //number of gets on remote keys in the transaction
   NUM_GET(true, false), //number of gets on local keys in the transaction
   NUM_REMOTE_PUT(true, false), //number of gets on remote keys in the transaction
   NUM_PUT(true, false), //number of gets on local keys in the transaction

   //Per operation execution time
   REMOTE_GET_EXECUTION(true, false), //get execution time for a remote keys
   LOCAL_GET_EXECUTION(true, false), //get execution time for a local keys
   ALL_GET_EXECUTION(true, false), //TODO: fixme
   REMOTE_PUT_EXECUTION(true, false), //put execution time for a remote key
   LOCAL_PUT_EXECUTION(true, false), //put execution time for a local key

   //Transaction counter
   //TODO add the remaining
   NUM_COMMITTED_TX(false, false), //the total number of committed remote and local transactions
   NUM_LOCAL_COMMITTED_TX(false, false), //the total number of committed local transactions

   //Lock related
   LOCK_HOLD_TIME(true, true), //the average lock hold time
   NUM_HELD_LOCKS(true, true), //the total number of locks acquired
   LOCK_HOLD_TIME_SUCCESS_LOCAL_TX(true, false), //the average lock hold time
   NUM_HELD_LOCKS_SUCCESS_LOCAL_TX(true, false), //number of locks held by a successful local transaction
   NUM_LOCK_FAILED_TIMEOUT(true, false), //number of locks failed to acquire due to timeout
   NUM_LOCK_FAILED_DEADLOCK(true, false), //number of locks failed to acquire due to deadlock (deadlock exception)
   NUM_WAITED_FOR_LOCKS(true, true), //number of lock that were contented and the transaction have to wait
   LOCK_WAITING_TIME(true, true), //the waiting time before acquire the lock

   LOCK_HOLD_TIME_LOCAL(false, false), //query only: average lock hold time by local transactions
   LOCK_HOLD_TIME_REMOTE(false, false), //query only: average lock hold time by remote transaction
   NUM_LOCK_PER_LOCAL_TX(false, false), //query only: average number of locks per all local transaction
   NUM_LOCK_PER_REMOTE_TX(false, false), //query only: average number of locks per all remote transaction

   //other transactional stuff
   WRITE_TX_PERCENTAGE(false, false), //query only: write transaction percentage
   SUCCESSFUL_WRITE_TX_PERCENTAGE(false, false), //query only: same as above but only for successful transactions

   //write skew
   NUM_WRITE_SKEW(true, false), //number of write skew detected
   WRITE_SKEW_PROBABILITY(false, false), //query only: write skew probability

   //Abort rate, arrival rate and throughput
   ABORT_RATE(false, false), //percentage of aborted transactions
   ARRIVAL_RATE(false, false), // number of local and remote transactions processed per second
   THROUGHPUT(false, false), //number of local transactions processed per second

   //commands execution time
   PREPARE_EXECUTION_TIME(true, true), //prepare command execution time (includes communication)
   NUM_PREPARE_COMMAND(true, true), //number of prepare commands
   LOCAL_PREPARE_EXECUTION_TIME(false, false), //query only: average prepare command execution time for local transactions
   REMOTE_PREPARE_EXECUTION_TIME(false, false), //query only: average prepare command execution time for remote transactions

   COMMIT_EXECUTION_TIME(true, true), //commit command execution time (includes communication)
   NUM_COMMIT_COMMAND(true, true), //number of commit commands
   LOCAL_COMMIT_EXECUTION_TIME(false, false), //query only: average commit command execution time for local transactions
   REMOTE_COMMIT_EXECUTION_TIME(false, false), //query only: average commit command execution time for remote transactions

   ROLLBACK_EXECUTION_TIME(true, true), //rollback command execution time (includes communication)
   NUM_ROLLBACK_COMMAND(true, true), //number of rollback commands
   LOCAL_ROLLBACK_EXECUTION_TIME(false, false), //query only: average rollback command execution time for local transactions
   REMOTE_ROLLBACK_EXECUTION_TIME(false, false), //query only: average rollback command execution time for remote transactions

   //commands size
   PREPARE_COMMAND_SIZE(true, false), //prepare command size in bytes
   COMMIT_COMMAND_SIZE(true, false), //commit command size in bytes
   CLUSTERED_GET_COMMAND_SIZE(true, false), //cluster get command size in bytes

   //RTT: local && synchronous communication
   NUM_SYNC_PREPARE(true, false), //number of synchronous prepare command sent
   SYNC_PREPARE_TIME(true, false), //response time of synchronous prepare command
   NUM_SYNC_COMMIT(true, false), //number of synchronous commit command sent
   SYNC_COMMIT_TIME(true, false), //response time of synchronous commit command
   NUM_SYNC_ROLLBACK(true, false), //number of synchronous rollback command sent
   SYNC_ROLLBACK_TIME(true, false), //response time of synchronous rollback command
   NUM_SYNC_GET(true, false), //number of synchronous cluster get command sent
   SYNC_GET_TIME(true, false), //response time of synchronous cluster get command

   //ASYNC: local && asynchronous communication
   ASYNC_PREPARE_TIME(true, false), //send-and-forget time for prepare command
   NUM_ASYNC_PREPARE(true, false), //number of asynchronous prepare command sent
   ASYNC_COMMIT_TIME(true, false), //send-and-forget time for commit command
   NUM_ASYNC_COMMIT(true, false), //number of asynchronous commit command sent
   ASYNC_ROLLBACK_TIME(true, false), //send-and-forget time for rollback command
   NUM_ASYNC_ROLLBACK(true, false), //number of asynchronous rollback command sent
   ASYNC_COMPLETE_NOTIFY_TIME(true, false), //send-and-forget time for tx completion notify command
   NUM_ASYNC_COMPLETE_NOTIFY(true, false), //number of asynchronous tx completion notify command sent

   //Number of nodes in remote communication
   NUM_NODES_PREPARE(true, false), //average number of nodes involved in a prepare command
   NUM_NODES_COMMIT(true, false), //average number of nodes involved in a commit command
   NUM_NODES_ROLLBACK(true, false), //average number of nodes involved in a rollback command
   NUM_NODES_COMPLETE_NOTIFY(true, false), //average number of nodes involved in a tx completion notify command
   NUM_NODES_GET(true, false), //average number of nodes involved in a tx completion notify command
   RESPONSE_TIME(false, false); //average transaction response time
   public static final int NO_INDEX = -1;
   private static short localStatsSize = 0;
   private static short remoteStatsSize = 0;
   private final boolean local;
   private final boolean remote;
   private short localIndex = NO_INDEX;
   private short remoteIndex = NO_INDEX;

   ExtendedStatistic(boolean local, boolean remote) {
      this.local = local;
      this.remote = remote;
   }

   public final int getLocalIndex() {
      return localIndex;
   }

   public final int getRemoteIndex() {
      return remoteIndex;
   }

   public final boolean isLocal() {
      return local;
   }

   public final boolean isRemote() {
      return remote;
   }

   public static int getRemoteStatsSize() {
      return remoteStatsSize;
   }

   public static int getLocalStatsSize() {
      return localStatsSize;
   }

   static {
      for (ExtendedStatistic stat : values()) {
         if (stat.local) {
            stat.localIndex = localStatsSize++;
         }
         if (stat.remote) {
            stat.remoteIndex = remoteStatsSize++;
         }
      }
   }
}
