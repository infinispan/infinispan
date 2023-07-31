package org.infinispan.util.concurrent;

import static java.lang.String.format;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import net.jcip.annotations.GuardedBy;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.ClusteringConfiguration;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.interceptors.distribution.Collector;
import org.infinispan.interceptors.distribution.PrimaryOwnerOnlyCollector;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * An acknowledge collector for Triangle algorithm used in non-transactional caches for write operations.
 * <p>
 * Acknowledges are used between the owners and the originator. They signal the completion of a write operation. The
 * operation can complete successfully or not.
 * <p>
 * The acknowledges are valid on the same cache topology id. So, each acknowledge is tagged with the command topology
 * id. Acknowledges from previous topology id are discarded.
 * <p>
 * The acknowledges from the primary owner carry the return value of the operation.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
@Scope(Scopes.NAMED_CACHE)
public class CommandAckCollector {

   private static final Log log = LogFactory.getLog(CommandAckCollector.class);

   @Inject @ComponentName(KnownComponentNames.TIMEOUT_SCHEDULE_EXECUTOR)
   ScheduledExecutorService timeoutExecutor;
   @Inject Configuration configuration;

   private final ConcurrentHashMap<Long, BaseCollector<?>> collectorMap;
   private long timeoutNanoSeconds;

   public CommandAckCollector() {
      collectorMap = new ConcurrentHashMap<>(64);
   }

   @Start
   public void start() {
      timeoutNanoSeconds = TimeUnit.MILLISECONDS.toNanos(configuration.clustering().remoteTimeout());
      configuration.clustering().attributes().attribute(ClusteringConfiguration.REMOTE_TIMEOUT)
            .addListener((a, ignored) -> timeoutNanoSeconds = TimeUnit.MILLISECONDS.toNanos(a.get()));
   }

   /**
    * Creates a collector for a single key write operation.
    *
    * @param id           the id from {@link CommandInvocationId}.
    * @param backupOwners the backup owners of the key.
    * @param topologyId   the current topology id.
    */
   public <T> Collector<T> create(long id, Collection<Address> backupOwners, int topologyId) {
      if (backupOwners.isEmpty()) {
         return new PrimaryOwnerOnlyCollector<>();
      }
      SingleKeyCollector<T> collector = new SingleKeyCollector<>(id, backupOwners, topologyId);
      BaseCollector<?> prev = collectorMap.put(id, collector);
      //is it possible to have a previous collector when the topology changes after the first collector is created
      //in that case, the previous collector must have a lower topology id
      assert prev == null || prev.topologyId < topologyId : format("replaced old collector '%s' by '%s'", prev, collector);
      if (log.isTraceEnabled()) {
         log.tracef("Created new collector for %s. BackupOwners=%s", id, backupOwners);
      }
      return collector;
   }

   /**
    * Creates a collector for {@link org.infinispan.commands.write.PutMapCommand}.
    *
    * @param id         the id from {@link CommandInvocationId#getId()}.
    * @param backups    a map between a backup owner and its segments affected.
    * @param topologyId the current topology id.
    */
   public <T> Collector<T> createSegmentBasedCollector(long id, Map<Address, Collection<Integer>> backups, int topologyId) {
      if (backups.isEmpty()) {
         return new PrimaryOwnerOnlyCollector<>();
      }
      SegmentBasedCollector<T> collector = new SegmentBasedCollector<>(id, backups, topologyId);
      BaseCollector<?> prev = collectorMap.put(id, collector);
      //is it possible to have a previous collector when the topology changes after the first collector is created
      //in that case, the previous collector must have a lower topology id
      assert prev == null || prev.topologyId < topologyId : format("replaced old collector '%s' by '%s'", prev, collector);
      if (log.isTraceEnabled()) {
         log.tracef("Created new collector for %s. BackupSegments=%s", id, backups);
      }
      return collector;
   }

   /**
    * Acknowledges a {@link org.infinispan.commands.write.PutMapCommand} completion in the backup owner.
    *
    * @param id         the id from {@link CommandInvocationId#getId()}.
    * @param from       the backup owner.
    * @param segment    the segments affected and acknowledged.
    * @param topologyId the topology id.
    */
   public void backupAck(long id, Address from, int segment, int topologyId) {
      BaseCollector<?> collector = collectorMap.get(id);
      if (collector != null) {
         collector.backupAck(from, segment, topologyId);
      }
   }

   /**
    * Acknowledges an exception during the operation execution.
    * <p>
    * The collector is completed without waiting any further acknowledges.
    *
    * @param id         the id from {@link CommandInvocationId#getId()}.
    * @param throwable  the {@link Throwable}.
    * @param topologyId the topology id.
    */
   public void completeExceptionally(long id, Throwable throwable, int topologyId) {
      BaseCollector<?> ackTarget = collectorMap.get(id);
      if (ackTarget != null) {
         ackTarget.completeExceptionally(throwable, topologyId);
      }
   }

   /**
    * @return the pending ids from {@link CommandInvocationId#getId()} (testing purposes only)
    */
   public List<Long> getPendingCommands() {
      return new ArrayList<>(collectorMap.keySet());
   }

   /**
    * @param id the id from {@link CommandInvocationId#getId()}.
    * @return {@code true} if there are acknowledges pending from the backup owners, {@code false} otherwise. (testing
    * purposes only)
    */
   @SuppressWarnings("BooleanMethodIsAlwaysInverted") //testing only
   public boolean hasPendingBackupAcks(long id) {
      BaseCollector<?> ackTarget = collectorMap.get(id);
      return ackTarget != null && ackTarget.hasPendingBackupAcks();
   }

   /**
    * Notifies a change in member list.
    *
    * @param members the new cluster members.
    */
   public void onMembersChange(Collection<Address> members) {
      Set<Address> currentMembers = new HashSet<>(members);
      for (BaseCollector<?> ackTarget : collectorMap.values()) {
         ackTarget.onMembersChange(currentMembers);
      }
   }

   private abstract class BaseCollector<T> implements Collector<T>, Callable<Void>, BiConsumer<T, Throwable> {

      final long id;
      final int topologyId;
      final ScheduledFuture<?> timeoutTask;
      final CompletableFuture<T> future;
      final CompletableFuture<T> exposedFuture;
      volatile T primaryResult;
      volatile boolean primaryResultReceived;

      BaseCollector(long id, int topologyId) {
         this.topologyId = topologyId;
         this.id = id;
         timeoutTask = timeoutExecutor.schedule(this, timeoutNanoSeconds, TimeUnit.NANOSECONDS);
         future = new CompletableFuture<>();
         exposedFuture = future.whenComplete(this);
      }

      /**
       * Invoked by the timeout executor when the timeout expires.
       * <p>
       * It completes the future with the timeout exception.
       */
      @Override
      public final synchronized Void call() {
         future.completeExceptionally(log.timeoutWaitingForAcks(Util.prettyPrintTime(timeoutNanoSeconds, TimeUnit.NANOSECONDS), getAddress(), id, topologyId));
         return null;
      }

      protected abstract String getAddress();

      @Override
      public final CompletableFuture<T> getFuture() {
         return exposedFuture;
      }

      @Override
      public void primaryException(Throwable throwable) {
         future.completeExceptionally(throwable);
      }

      final void completeExceptionally(Throwable throwable, int topologyId) {
         if (log.isTraceEnabled()) {
            log.tracef(throwable, "[Collector#%s] completed exceptionally. TopologyId=%s (expected=%s)",
                  id, topologyId, this.topologyId);
         }
         if (isWrongTopologyOrIsDone(topologyId)) {
            return;
         }
         future.completeExceptionally(throwable);
      }

      final boolean isWrongTopologyOrIsDone(int topologyId) {
         return this.topologyId != topologyId || future.isDone();
      }

      /**
       * Invoked when the collector's future is completed, it must cleanup all task related to this collector.
       * <p>
       * The tasks includes removing the collector from the map and cancel the timeout task.
       */
      public final void accept(T t, Throwable throwable) {
         if (log.isTraceEnabled()) {
            log.tracef("[Collector#%s] Collector completed with ret=%s, throw=%s", id, t, throwable);
         }
         boolean removed = collectorMap.remove(id, this);
         assert removed;
         timeoutTask.cancel(false);
      }

      abstract boolean hasPendingBackupAcks();

      abstract void onMembersChange(Collection<Address> members);

      abstract void backupAck(Address from, int segment, int topologyId);
   }

   private class SingleKeyCollector<T> extends BaseCollector<T> {
      @GuardedBy("this")
      final Collection<Address> backupOwners;

      private SingleKeyCollector(long id, Collection<Address> backupOwners, int topologyId) {
         super(id, topologyId);
         this.backupOwners = new HashSet<>(backupOwners);
      }

      @Override
      synchronized boolean hasPendingBackupAcks() {
         if (log.isTraceEnabled()) {
            log.tracef("Pending backup acks: %s", backupOwners);
         }
         return !backupOwners.isEmpty();
      }

      @Override
      void onMembersChange(Collection<Address> members) {
         boolean empty;
         synchronized (this) {
            empty = backupOwners.retainAll(members) && backupOwners.isEmpty();
         }
         if (empty && primaryResultReceived) {
            if (log.isTraceEnabled()) {
               log.tracef("[Collector#%s] Some backups left the cluster.", id);
            }
            markReady();
         }
      }

      @Override
      public void primaryResult(T result, boolean success) {
         primaryResult = result;
         primaryResultReceived = true;
         if (!success || !hasPendingBackupAcks()) {
            markReady();
         }
      }

      @Override
      void backupAck(Address from, int segment, int topologyId) {
         assert segment >= 0;
         if (log.isTraceEnabled()) {
            log.tracef("[Collector#%s] Backup ACK. Address=%s, TopologyId=%s (expected=%s)",
                  id, from, topologyId, this.topologyId);
         }
         if (isWrongTopologyOrIsDone(topologyId)) {
            return;
         }
         boolean empty;
         synchronized (this) {
            empty = backupOwners.remove(from) && backupOwners.isEmpty();
         }
         if (empty && primaryResultReceived) {
            markReady();
         }
      }

      void markReady() {
         if (log.isTraceEnabled()) {
            log.tracef("[Collector#%s] Ready!", id);
         }
         future.complete(primaryResult);
      }

      @Override
      protected synchronized String getAddress() {
         return backupOwners.toString();
      }

      @Override
      public synchronized String toString() {
         return "SingleKeyCollector{" +
               "id=" + id +
               ", topologyId=" + topologyId +
               ", primaryResult=" + primaryResult +
               ", primaryResultReceived=" + primaryResultReceived +
               ", backupOwners=" + backupOwners +
               '}';
      }
   }

   private class SegmentBasedCollector<T> extends BaseCollector<T> {
      @GuardedBy("this")
      private final Map<Address, Collection<Integer>> backups;

      SegmentBasedCollector(long id, Map<Address, Collection<Integer>> backups,
                            int topologyId) {
         super(id, topologyId);
         this.backups = backups;
      }

      @Override
      public synchronized boolean hasPendingBackupAcks() {
         return !backups.isEmpty();
      }

      @Override
      public synchronized void onMembersChange(Collection<Address> members) {
         if (backups.keySet().retainAll(members)) {
            if (log.isTraceEnabled()) {
               log.tracef("[Collector#%s] Some backups left the cluster.", id);
            }
            checkCompleted();
         }
      }

      @Override
      public void primaryResult(T result, boolean success) {
         primaryResult = result;
         primaryResultReceived = true;
         synchronized (this) {
            checkCompleted();
         }
      }

      @Override
      void backupAck(Address from, int segment, int topologyId) {
         assert segment >= 0;
         if (log.isTraceEnabled()) {
            log.tracef("[Collector#%s] PutMap Backup ACK. Address=%s. TopologyId=%s (expected=%s). Segment=%s",
                  id, from, topologyId, this.topologyId, segment);
         }
         if (isWrongTopologyOrIsDone(topologyId)) {
            return;
         }
         synchronized (this) {
            Collection<Integer> pendingSegments = backups.getOrDefault(from, Collections.emptyList());
            if (pendingSegments.remove(segment) && pendingSegments.isEmpty()) {
               backups.remove(from);
               checkCompleted();
            }
         }
      }

      @GuardedBy("this")
      private void checkCompleted() {
         if (primaryResultReceived && backups.isEmpty()) {
            if (log.isTraceEnabled()) {
               log.tracef("[Collector#%s] Ready! Return value=%ss.", id, primaryResult);
            }
            future.complete(primaryResult);
         }
      }

      @Override
      protected synchronized String getAddress() {
         return backups.keySet().toString();
      }

      @Override
      public synchronized String toString() {
         return "SegmentBasedCollector{" + "id=" + id +
               ", topologyId=" + topologyId +
               ", primaryResult=" + primaryResult +
               ", primaryResultReceived=" + primaryResultReceived +
               ", backups=" + backups +
               '}';
      }
   }
}
