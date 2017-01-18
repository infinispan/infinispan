package org.infinispan.util.concurrent;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
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

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commons.util.Util;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.remoting.transport.Address;
import org.infinispan.statetransfer.OutdatedTopologyException;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import net.jcip.annotations.GuardedBy;

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
@Scope(Scopes.GLOBAL)
public class CommandAckCollector {

   private static final Log log = LogFactory.getLog(CommandAckCollector.class);
   private static final boolean trace = log.isTraceEnabled();

   private final ConcurrentHashMap<Long, Collector<?>> collectorMap;
   private ScheduledExecutorService timeoutExecutor;

   public CommandAckCollector() {
      collectorMap = new ConcurrentHashMap<>();
   }

   private static TimeoutException createTimeoutException(long timeoutNanoSeconds) {
      return log.timeoutWaitingForAcks(Util.prettyPrintTime(timeoutNanoSeconds, TimeUnit.NANOSECONDS));
   }

   @Inject
   public void inject(
         @ComponentName(value = KnownComponentNames.TIMEOUT_SCHEDULE_EXECUTOR) ScheduledExecutorService timeoutExecutor) {
      this.timeoutExecutor = timeoutExecutor;
   }

   /**
    * Creates a collector for a single key write operation.
    *
    * @param id           the id from {@link CommandInvocationId#getId()}.
    * @param owners       the owners of the key. It assumes the first element as primary owner.
    * @param topologyId   the current topology id.
    * @param timeoutNanos the timeout in nano seconds.
    */
   public void create(Long id, Collection<Address> owners, int topologyId, long timeoutNanos) {
      if (owners.isEmpty()) {
         if (trace) {
            log.tracef("No backup owners. Skip collector creation for %s", id);
         }
         return;
      }
      collectorMap.putIfAbsent(id, new SingleKeyCollector(id, owners, topologyId, timeoutNanos));
      if (trace) {
         log.tracef("Created new collector for %s. Owners=%s", id, owners);
      }
   }

   /**
    * Creates a collector for {@link org.infinispan.commands.write.PutMapCommand}.
    *
    * @param id           the id from {@link CommandInvocationId#getId()}.
    * @param primary      a primary owners collection..
    * @param backups      a map between a backup owner and its segments affected.
    * @param topologyId   the current topology id.
    * @param timeoutNanos the timeout in nano seconds.
    */
   public void createMultiKeyCollector(Long id, Collection<Address> primary,
         Map<Address, Collection<Integer>> backups, int topologyId, long timeoutNanos) {
      collectorMap.putIfAbsent(id, new MultiKeyCollector(id, primary, backups, topologyId, timeoutNanos));
      if (trace) {
         log.tracef("Created new collector for %s. Primary=%s. BackupSegments=%s", id, primary, backups);
      }
   }

   /**
    * Acknowledges a {@link org.infinispan.commands.write.PutMapCommand} completion in the primary owner.
    *
    * @param id          the id from {@link CommandInvocationId#getId()}.
    * @param from        the primary owner.
    * @param returnValue the return value.
    * @param topologyId  the topology id.
    */
   public void multiKeyPrimaryAck(long id, Address from, Map<Object, Object> returnValue,
         int topologyId) {
      MultiKeyCollector collector = (MultiKeyCollector) collectorMap.get(id);
      if (collector != null) {
         collector.primaryAck(returnValue, from, topologyId);
      }
   }

   /**
    * Acknowledges a {@link org.infinispan.commands.write.PutMapCommand} completion in the backup owner.
    *
    * @param id         the id from {@link CommandInvocationId#getId()}.
    * @param from       the backup owner.
    * @param segment    the segments affected and acknowledged.
    * @param topologyId the topology id.
    */
   public void multiKeyBackupAck(long id, Address from, int segment, int topologyId) {
      MultiKeyCollector collector = (MultiKeyCollector) collectorMap.get(id);
      if (collector != null) {
         collector.backupAck(from, segment, topologyId);
      }
   }

   /**
    * Acknowledges a write operation completion in the backup owner.
    *
    * @param id         the id from {@link CommandInvocationId#getId()}.
    * @param from       the backup owner.
    * @param topologyId the topology id.
    */
   public void backupAck(long id, Address from, int topologyId) {
      SingleKeyCollector collector = (SingleKeyCollector) collectorMap.get(id);
      if (collector != null) {
         collector.backupAck(topologyId, from);
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
      Collector<?> collector = collectorMap.get(id);
      if (collector != null) {
         collector.completeExceptionally(throwable, topologyId);
      }
   }

   /**
    * Returns the {@link CompletableFuture} associated tot the collector.
    *
    * @param <T> the type of the return value.
    * @param id  the id from {@link CommandInvocationId#getId()}.
    * @return the collector's {@link CompletableFuture}.
    */
   public <T> CompletableFuture<T> getCollectorCompletableFuture(long id) {
      //noinspection unchecked
      Collector<T> collector = (Collector<T>) collectorMap.get(id);
      return collector == null ? null : collector.getFuture();
   }

   /**
    * Returns the {@link CompletableFuture} associated tot the collector.
    * <p>
    * The collector is cleanup after the {@link CompletableFuture} is completed and it register a timeout task.
    *
    * @param <T> the type of the return value.
    * @param id  the id from {@link CommandInvocationId#getId()}.
    * @return the collector's {@link CompletableFuture}.
    */
   public <T> CompletableFuture<T> getCollectorCompletableFutureToWait(long id) {
      //noinspection unchecked
      Collector<T> collector = (Collector<T>) collectorMap.get(id);
      if (trace) {
         log.tracef("[Collector#%s] Waiting for acks asynchronously.", id);
      }
      return collector == null ? null : collector.addCleanupTasksAndGetFuture();
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
   public boolean hasPendingBackupAcks(long id) {
      Collector<?> collector = collectorMap.get(id);
      return collector != null && collector.hasPendingBackupAcks();
   }

   /**
    * Notifies a change in member list.
    *
    * @param members the new cluster members.
    */
   public void onMembersChange(Collection<Address> members) {
      Set<Address> currentMembers = new HashSet<>(members);
      for (Collector<?> collector : collectorMap.values()) {
         collector.onMembersChange(currentMembers);
      }
   }

   /**
    * Removes the collector associated with the command.
    *
    * @param id the id from {@link CommandInvocationId#getId()}.
    */
   public void dispose(long id) {
      if (trace) {
         log.tracef("[Collector#%s] Dispose collector.", id);
      }
      collectorMap.remove(id);
   }

   public void unsuccessfulCommand(long id) {
      Collector<?> collector = collectorMap.remove(id);
      if (collector != null) {
         collector.getFuture().complete(null);
      }
   }
   private abstract class Collector<T> implements Callable<Void>, BiConsumer<T, Throwable> {

      final Long id;
      final CompletableFuture<T> future;
      final int topologyId;
      private final long timeoutNanoSeconds;
      private volatile ScheduledFuture<?> timeoutTask;

      Collector(Long id, int topologyId, long timeoutNanoSeconds) {
         this.id = id;
         this.topologyId = topologyId;
         this.timeoutNanoSeconds = timeoutNanoSeconds;
         this.future = new CompletableFuture<>();
      }

      /**
       * Invoked by the timeout executor when the timeout expires.
       * <p>
       * It completes the future with the timeout exception.
       */
      @Override
      public final synchronized Void call() throws Exception {
         doCompleteExceptionally(createTimeoutException(timeoutNanoSeconds));
         return null;
      }

      /**
       * Invoked when the future is completed, it must cleanup all task related to this collector.
       * <p>
       * The tasks includes removing the collector from the map and cancel the timeout task.
       */
      @Override
      public final void accept(T t, Throwable throwable) {
         collectorMap.remove(id);
         timeoutTask.cancel(false);
      }

      synchronized final void completeExceptionally(Throwable throwable, int topologyId) {
         if (trace) {
            log.tracef(throwable, "[Collector#%s] completed exceptionally. TopologyId=%s (expected=%s)",
                  (Object) id, topologyId, this.topologyId);
         }
         if (this.topologyId != topologyId) {
            return;
         }
         doCompleteExceptionally(throwable);
      }

      abstract boolean hasPendingBackupAcks();

      final CompletableFuture<T> getFuture() {
         return future;
      }

      abstract void onMembersChange(Collection<Address> members);

      abstract void doCompleteExceptionally(Throwable throwable);

      final CompletableFuture<T> addCleanupTasksAndGetFuture() {
         if (future.isDone()) {
            collectorMap.remove(id);
            return future;
         }
         this.timeoutTask = timeoutExecutor.schedule(this, timeoutNanoSeconds, TimeUnit.NANOSECONDS);
         return future.whenComplete(this);
      }
   }

   private class SingleKeyCollector extends Collector<Object> {
      @GuardedBy("this")
      private final HashSet<Address> owners;

      private SingleKeyCollector(Long id, Collection<Address> owners, int topologyId, long timeoutNanos) {
         super(id, topologyId, timeoutNanos);
         this.owners = new HashSet<>(owners); //removal is fast
      }

      @Override
      public synchronized boolean hasPendingBackupAcks() {
         return !owners.isEmpty();
      }

      @Override
      public synchronized void onMembersChange(Collection<Address> members) {
         if (owners.retainAll(members) && owners.isEmpty()) {
            if (trace) {
               log.tracef("[Collector#%s] Some backups left the cluster.", id);
            }
            markReady();
         }
      }

      synchronized void backupAck(int topologyId, Address from) {
         if (trace) {
            traceBackupAck(topologyId, from);
         }
         if (this.topologyId == topologyId && owners.remove(from) && owners.isEmpty()) {
            markReady();
         }
      }

      @GuardedBy("this")
      void doCompleteExceptionally(Throwable throwable) {
         owners.clear();
         future.completeExceptionally(throwable);
      }

      @GuardedBy("this")
      private void markReady() {
         if (trace) {
            traceReady();
         }
         future.complete(null);
      }

      private void traceReady() {
         log.tracef("[Collector#%s] Ready!", id);
      }

      private void traceBackupAck(int topologyId, Address from) {
         log.tracef("[Collector#%s] Backup ACK. Address=%s, TopologyId=%s (expected=%s)",
               id, from, topologyId, this.topologyId);
      }
   }

   private class MultiKeyCollector extends Collector<Map<Object, Object>> {
      @GuardedBy("this")
      private final HashSet<Address> primary;
      @GuardedBy("this")
      private final HashMap<Address, Collection<Integer>> backups;
      @GuardedBy("this")
      private HashMap<Object, Object> returnValue;

      MultiKeyCollector(Long id, Collection<Address> primary, Map<Address, Collection<Integer>> backups,
            int topologyId, long timeoutNanos) {
         super(id, topologyId, timeoutNanos);
         this.returnValue = null;
         this.backups = new HashMap<>(backups);
         this.primary = new HashSet<>(primary);
      }

      @Override
      public synchronized boolean hasPendingBackupAcks() {
         return !backups.isEmpty();
      }

      @Override
      public synchronized void onMembersChange(Collection<Address> members) {
         if (!members.containsAll(primary)) {
            //primary owner left. throw OutdatedTopologyException to trigger a retry
            if (trace) {
               log.tracef("[Collector#%s] A primary Owner left the cluster.", id);
            }
            doCompleteExceptionally(OutdatedTopologyException.getCachedInstance());
         } else if (backups.keySet().retainAll(members)) {
            if (trace) {
               log.tracef("[Collector#%s] Some backups left the cluster.", id);
            }
            checkCompleted();
         }
      }

      synchronized void primaryAck(Map<Object, Object> returnValue, Address from, int topologyId) {
         if (trace) {
            log.tracef("[Collector#%s] PutMap Primary ACK. Address=%s. TopologyId=%s (expected=%s)",
                  id, from, topologyId, this.topologyId);
         }
         if (this.topologyId != topologyId) {
            return;
         }
         if (returnValue != null) {
            if (this.returnValue == null) {
               this.returnValue = new HashMap<>(returnValue.size());
            }
            this.returnValue.putAll(returnValue);
         }
         if (primary.remove(from)) {
            checkCompleted();
         }
      }

      synchronized void backupAck(Address from, int segment, int topologyId) {
         if (trace) {
            log.tracef("[Collector#%s] PutMap Backup ACK. Address=%s. TopologyId=%s (expected=%s). Segment=%s",
                  id, from, topologyId, this.topologyId, segment);
         }
         if (this.topologyId != topologyId) {
            return;
         }
         Collection<Integer> pendingSegments = backups.getOrDefault(from, Collections.emptyList());
         if (pendingSegments.remove(segment) && pendingSegments.isEmpty()) {
            backups.remove(from);
            checkCompleted();
         }
      }

      @GuardedBy("this")
      void doCompleteExceptionally(Throwable throwable) {
         returnValue = null;
         primary.clear();
         backups.clear();
         future.completeExceptionally(throwable);
      }

      @GuardedBy("this")
      private void checkCompleted() {
         if (primary.isEmpty() && backups.isEmpty()) {
            if (trace) {
               log.tracef("[Collector#%s] Ready! Return value=%ss.", id, returnValue);
            }
            future.complete(returnValue);
         }
      }
   }
}
