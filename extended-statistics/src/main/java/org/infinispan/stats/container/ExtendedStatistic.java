package org.infinispan.stats.container;

/**
 * The available extended statistics
 *
 * @author Roberto Palmieri
 * @author Sebastiano Peluso
 * @author Diego Didona
 * @author Pedro Ruivo
 * @since 5.3
 */

public enum ExtendedStatistic {

   WR_TX_LOCAL_EXECUTION_TIME(true, false),
   NUM_COMMITTED_RO_TX(true, true),
   NUM_COMMITTED_WR_TX(true, true),
   NUM_ABORTED_WR_TX(true, true),
   NUM_ABORTED_RO_TX(true, true),
   NUM_COMMITS(false, false),
   NUM_LOCAL_COMMITS(false, false),
   NUM_PREPARES(true, false),
   LOCAL_EXEC_NO_CONT(true, false),
   LOCK_HOLD_TIME_LOCAL(false, false),
   LOCK_HOLD_TIME_REMOTE(false, false),
   NUM_SUCCESSFUL_PUTS(true, false),
   PUTS_PER_LOCAL_TX(false, false),
   NUM_WAITED_FOR_LOCKS(true, true),
   NUM_REMOTE_GET(true, true),
   NUM_GET(true, true),
   NUM_SUCCESSFUL_GETS_RO_TX(true, true),
   NUM_SUCCESSFUL_GETS_WR_TX(true, true),
   NUM_SUCCESSFUL_REMOTE_GETS_WR_TX(true, true),
   NUM_SUCCESSFUL_REMOTE_GETS_RO_TX(true, true),
   LOCAL_GET_EXECUTION(true, true),
   ALL_GET_EXECUTION(true, true),
   REMOTE_GET_EXECUTION(true, true),
   REMOTE_PUT_EXECUTION(true, true),
   NUM_REMOTE_PUT(true, true),
   NUM_PUT(true, true),
   NUM_SUCCESSFUL_PUTS_WR_TX(true, true),
   NUM_SUCCESSFUL_REMOTE_PUTS_WR_TX(true, true),
   TX_WRITE_PERCENTAGE(false, false),
   SUCCESSFUL_WRITE_PERCENTAGE(false, false),
   WR_TX_ABORTED_EXECUTION_TIME(true, true),
   WR_TX_SUCCESSFUL_EXECUTION_TIME(true, true),
   RO_TX_SUCCESSFUL_EXECUTION_TIME(true, true),
   RO_TX_ABORTED_EXECUTION_TIME(true, true),
   NUM_WRITE_SKEW(true, false),
   WRITE_SKEW_PROBABILITY(false, false),
   //Abort rate, arrival rate and throughput
   ABORT_RATE(false, false),
   ARRIVAL_RATE(false, false),
   THROUGHPUT(false, false),
   //Prepare, rollback and commit execution times
   ROLLBACK_EXECUTION_TIME(true, true),
   NUM_ROLLBACKS(true, true),
   LOCAL_ROLLBACK_EXECUTION_TIME(false, false),
   REMOTE_ROLLBACK_EXECUTION_TIME(false, false),
   COMMIT_EXECUTION_TIME(true, true),
   NUM_COMMIT_COMMAND(true, true),
   LOCAL_COMMIT_EXECUTION_TIME(false, false),
   REMOTE_COMMIT_EXECUTION_TIME(false, false),
   PREPARE_EXECUTION_TIME(true, true),
   NUM_PREPARE_COMMAND(true, true),
   LOCAL_PREPARE_EXECUTION_TIME(false, false),
   REMOTE_PREPARE_EXECUTION_TIME(false, false),
   NUM_LOCK_PER_LOCAL_TX(false, false),
   NUM_LOCK_PER_REMOTE_TX(false, false),
   NUM_LOCK_PER_SUCCESS_LOCAL_TX(false, false),
   LOCK_WAITING_TIME(true, true),
   LOCK_HOLD_TIME(true, true),
   NUM_HELD_LOCKS(true, true),
   NUM_HELD_LOCKS_SUCCESS_TX(true, false),
   //commands size
   PREPARE_COMMAND_SIZE(true, false),
   COMMIT_COMMAND_SIZE(true, false),
   CLUSTERED_GET_COMMAND_SIZE(true, false),
   //Lock failed stuff
   NUM_LOCK_FAILED_TIMEOUT(true, false),
   NUM_LOCK_FAILED_DEADLOCK(true, false),
   //RTT STUFF: everything is local && synchronous communication
   NUM_RTTS_PREPARE(true, false),
   RTT_PREPARE(true, false),
   NUM_RTTS_COMMIT(true, false),
   RTT_COMMIT(true, false),
   NUM_RTTS_ROLLBACK(true, false),
   RTT_ROLLBACK(true, false),
   NUM_RTTS_GET(true, false),
   RTT_GET(true, false),
   //SEND STUFF: everything is local && asynchronous communication
   ASYNC_PREPARE(true, false),
   NUM_ASYNC_PREPARE(true, false),
   ASYNC_COMMIT(true, false),
   NUM_ASYNC_COMMIT(true, false),
   ASYNC_ROLLBACK(true, false),
   NUM_ASYNC_ROLLBACK(true, false),
   ASYNC_COMPLETE_NOTIFY(true, false),
   NUM_ASYNC_COMPLETE_NOTIFY(true, false),
   //Number of nodes involved stuff
   NUM_NODES_PREPARE(true, false),
   NUM_NODES_COMMIT(true, false),
   NUM_NODES_ROLLBACK(true, false),
   NUM_NODES_COMPLETE_NOTIFY(true, false),
   NUM_NODES_GET(true, false),
   RESPONSE_TIME(false, false);
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
