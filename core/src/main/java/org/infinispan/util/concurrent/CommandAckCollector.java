package org.infinispan.util.concurrent;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.remoting.transport.Address;
import org.infinispan.statetransfer.OutdatedTopologyException;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

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
public class CommandAckCollector {

   private static final Log log = LogFactory.getLog(CommandAckCollector.class);
   private static final boolean trace = log.isTraceEnabled();

   private final ConcurrentHashMap<CommandInvocationId, Collector<?>> collectorMap;
   private ScheduledExecutorService timeoutExecutor;
   private long timeoutNanoSeconds;

   public CommandAckCollector() {
      collectorMap = new ConcurrentHashMap<>();
   }

   @Inject
   public void inject(
         @ComponentName(value = KnownComponentNames.TIMEOUT_SCHEDULE_EXECUTOR) ScheduledExecutorService timeoutExecutor,
         Configuration configuration) {
      this.timeoutExecutor = timeoutExecutor;
      this.timeoutNanoSeconds = TimeUnit.MILLISECONDS.toNanos(configuration.clustering().remoteTimeout());
   }

   /**
    * Creates a collector for a single key write operation.
    *
    * @param id         the {@link CommandInvocationId}.
    * @param owners     the owners of the key. It assumes the first element as primary owner.
    * @param topologyId the current topology id.
    */
   public void create(CommandInvocationId id, Collection<Address> owners, int topologyId) {
      collectorMap.putIfAbsent(id, new SingleKeyCollector(id, owners, topologyId));
      if (trace) {
         log.tracef("Created new collector for %s. Owners=%s", id, owners);
      }
   }

   /**
    * Creates a collector for a single key write operation.
    * <p>
    * It should be used when the primary owner is the local node and the return value and its acknowledge is already
    * known.
    *
    * @param id          the {@link CommandInvocationId}.
    * @param returnValue the primary owner result.
    * @param owners      the owners of the key. It assumes the first element as primary owner.
    * @param topologyId  the current topology id.
    */
   public void create(CommandInvocationId id, Object returnValue, Collection<Address> owners, int topologyId) {
      collectorMap.putIfAbsent(id, new SingleKeyCollector(id, returnValue, owners, topologyId));
      if (trace) {
         log.tracef("Created new collector for %s. ReturnValue=%s. Owners=%s", id, returnValue, owners);
      }
   }

   /**
    * Creates a collector for {@link org.infinispan.commands.write.PutMapCommand}.
    *
    * @param id         the {@link CommandInvocationId}.
    * @param primary    a primary owners collection..
    * @param backups    a map between a backup owner and its segments affected.
    * @param topologyId the current topology id.
    */
   public void createMultiKeyCollector(CommandInvocationId id, Collection<Address> primary,
         Map<Address, Collection<Integer>> backups, int topologyId) {
      collectorMap.putIfAbsent(id, new MultiKeyCollector(id, primary, backups, topologyId));
      if (trace) {
         log.tracef("Created new collector for %s. PrimarySegments=%s. BackupSegments", id, primary, backups);
      }
   }

   /**
    * Acknowledges a {@link org.infinispan.commands.write.PutMapCommand} completion in the primary owner.
    *
    * @param id          the {@link CommandInvocationId}.
    * @param from        the primary owner.
    * @param returnValue the return value.
    * @param topologyId  the topology id.
    */
   public void multiKeyPrimaryAck(CommandInvocationId id, Address from, Map<Object, Object> returnValue,
         int topologyId) {
      MultiKeyCollector collector = (MultiKeyCollector) collectorMap.get(id);
      if (collector != null) {
         collector.primaryAck(returnValue, from, topologyId);
      }
   }

   /**
    * Acknowledges a {@link org.infinispan.commands.write.PutMapCommand} completion in the backup owner.
    *
    * @param id         the {@link CommandInvocationId}.
    * @param from       the backup owner.
    * @param segments   the segments affected and acknowledged.
    * @param topologyId the topology id.
    */
   public void multiKeyBackupAck(CommandInvocationId id, Address from, Collection<Integer> segments, int topologyId) {
      MultiKeyCollector collector = (MultiKeyCollector) collectorMap.get(id);
      if (collector != null) {

         collector.backupAck(from, segments, topologyId);
      }
   }

   /**
    * Acknowledges a write operation completion in the backup owner.
    *
    * @param id         the {@link CommandInvocationId}.
    * @param from       the backup owner.
    * @param topologyId the topology id.
    */
   public void backupAck(CommandInvocationId id, Address from, int topologyId) {
      SingleKeyCollector collector = (SingleKeyCollector) collectorMap.get(id);
      if (collector != null) {
         collector.backupAck(topologyId, from);
      }
   }

   /**
    * Acknowledges a write operation completion in the primary owner.
    * <p>
    * If the operation does not succeed (conditional commands), the collector is completed without waiting for the
    * acknowledges from the backup owners.
    *
    * @param id          the {@link CommandInvocationId}.
    * @param returnValue the return value.
    * @param success     {@code true} if the operation succeed in the primary owner, {@code false} otherwise.
    * @param from        the primary owner.
    * @param topologyId  the topology id.
    */
   public void primaryAck(CommandInvocationId id, Object returnValue, boolean success, Address from, int topologyId) {
      SingleKeyCollector collector = (SingleKeyCollector) collectorMap.get(id);
      if (collector != null) {
         collector.primaryAck(topologyId, returnValue, success, from);
      }
   }

   /**
    * Acknowledges an exception during the operation execution.
    * <p>
    * The collector is completed without waiting any further acknowledges.
    *
    * @param id         the {@link CommandInvocationId}.
    * @param throwable  the {@link Throwable}.
    * @param topologyId the topology id.
    */
   public void completeExceptionally(CommandInvocationId id, Throwable throwable, int topologyId) {
      Collector<?> collector = collectorMap.get(id);
      if (collector != null) {
         collector.completeExceptionally(throwable, topologyId);
      }
   }

   /**
    * Returns the {@link CompletableFuture} associated tot the collector.
    *
    * @param <T> the type of the return value.
    * @param id  the {@link CommandInvocationId}.
    * @return the collector's {@link CompletableFuture}.
    */
   public <T> CompletableFuture<T> getCollectorCompletableFuture(CommandInvocationId id) {
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
    * @param id  the {@link CommandInvocationId}.
    * @return the collector's {@link CompletableFuture}.
    */
   public <T> CompletableFuture<T> getCollectorCompletableFutureToWait(CommandInvocationId id) {
      //noinspection unchecked
      Collector<T> collector = (Collector<T>) collectorMap.get(id);
      if (collector == null) {
         return null;
      }
      if (trace) {
         log.tracef("[Collector#%s] Waiting for acks asynchronously.", id);
      }
      return collector.addCleanupTasksAndGetFuture();
   }

   /**
    * @return the pending {@link CommandInvocationId} (testing purposes only)
    */
   public List<CommandInvocationId> getPendingCommands() {
      return new ArrayList<>(collectorMap.keySet());
   }

   /**
    * @param id the command id.
    * @return {@code true} if there are acknowledges pending from the backup owners, {@code false} otherwise. (testing
    * purposes only)
    */
   public boolean hasPendingBackupAcks(CommandInvocationId id) {
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
    * @param id the {@link CommandInvocationId}.
    */
   public void dispose(CommandInvocationId id) {
      if (trace) {
         log.tracef("[Collector#%s] Dispose collector.", id);
      }
      collectorMap.remove(id);
   }

   private TimeoutException createTimeoutException() {
      return log.timeoutWaitingForAcks(Util.prettyPrintTime(timeoutNanoSeconds, TimeUnit.NANOSECONDS));
   }

   private abstract class Collector<T> implements Callable<Void>, BiConsumer<T, Throwable> {

      protected final CommandInvocationId id;
      protected final CompletableFuture<T> future;
      protected final int topologyId;
      private volatile ScheduledFuture<?> timeoutTask;

      protected Collector(CommandInvocationId id, int topologyId) {
         this.id = id;
         this.topologyId = topologyId;
         this.future = new CompletableFuture<>();
      }

      /**
       * Invoked by the timeout executor when the timeout expires.
       * <p>
       * It completes the future with the timeout exception.
       */
      @Override
      public final synchronized Void call() throws Exception {
         doCompleteExceptionally(createTimeoutException());
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
                  id, topologyId, this.topologyId);
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
      private final Collection<Address> owners;
      @GuardedBy("this")
      private final Address primaryOwner;
      @GuardedBy("this")
      private Object returnValue;

      private SingleKeyCollector(CommandInvocationId id, Collection<Address> owners, int topologyId) {
         super(id, topologyId);
         this.primaryOwner = owners.iterator().next();
         this.owners = new HashSet<>(owners); //removal is fast
      }

      private SingleKeyCollector(CommandInvocationId id, Object returnValue, Collection<Address> owners,
            int topologyId) {
         super(id, topologyId);
         this.returnValue = returnValue;
         this.primaryOwner = owners.iterator().next();
         Collection<Address> tmpOwners = new HashSet<>(owners);
         tmpOwners.remove(primaryOwner);
         if (tmpOwners.isEmpty()) { //num owners is 1 or single member in cluster
            this.owners = Collections.emptyList();
            this.future.complete(returnValue);
         } else {
            this.owners = tmpOwners;
         }
      }

      @Override
      public synchronized boolean hasPendingBackupAcks() {
         return owners.size() > 1 || //at least one backup + primary address
               //if one is missing, make sure that it isn't the primary
               owners.size() == 1 && !primaryOwner.equals(owners.iterator().next());
      }

      @Override
      public synchronized void onMembersChange(Collection<Address> members) {
         if (!members.contains(primaryOwner)) {
            //primary owner left. throw OutdatedTopologyException to trigger a retry
            if (trace) {
               log.tracef("[Collector#%s] The Primary Owner left the cluster.", id);
            }
            doCompleteExceptionally(OutdatedTopologyException.getCachedInstance());
         } else if (owners.retainAll(members) && owners.isEmpty()) {
            if (trace) {
               log.tracef("[Collector#%s] Some backups left the cluster.", id);
            }
            markReady();
         }
      }

      synchronized void primaryAck(int topologyId, Object returnValue, boolean success, Address from) {
         if (trace) {
            log.tracef(
                  "[Collector#%s] Primary ACK. Success=%s. ReturnValue=%s. Address=%s, TopologyId=%s (expected=%s)",
                  id, success, returnValue, from, topologyId, this.topologyId);
         }
         if (this.topologyId != topologyId || !owners.remove(from)) {
            //already received!
            return;
         }
         this.returnValue = returnValue;

         if (!success) {
            //we are not receiving any backups ack!
            owners.clear();
            future.complete(returnValue);
            if (trace) {
               log.tracef("[Collector#%s] Ready! Command not succeed on primary.", id);
            }
         } else if (owners.isEmpty()) {
            markReady();
         }
      }

      synchronized void backupAck(int topologyId, Address from) {
         if (trace) {
            log.tracef("[Collector#%s] Backup ACK. Address=%s, TopologyId=%s (expected=%s)",
                  id, from, topologyId, this.topologyId);
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
            log.tracef("[Collector#%s] Ready! Return value=%ss.", id, returnValue);
         }
         future.complete(returnValue);
      }
   }

   private class MultiKeyCollector extends Collector<Map<Object, Object>> {
      @GuardedBy("this")
      private final Collection<Address> primary;
      @GuardedBy("this")
      private final Map<Address, Collection<Integer>> backups;
      @GuardedBy("this")
      private Map<Object, Object> returnValue;

      MultiKeyCollector(CommandInvocationId id, Collection<Address> primary, Map<Address, Collection<Integer>> backups,
            int topologyId) {
         super(id, topologyId);
         this.returnValue = null;
         this.backups = backups;
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

      synchronized void backupAck(Address from, Collection<Integer> segments, int topologyId) {
         if (trace) {
            log.tracef("[Collector#%s] PutMap Backup ACK. Address=%s. TopologyId=%s (expected=%s). Segments=%s",
                  id, from, topologyId, this.topologyId, segments);
         }
         if (this.topologyId != topologyId) {
            return;
         }
         Collection<Integer> pendingSegments = backups.getOrDefault(from, Collections.emptyList());
         if (pendingSegments.removeAll(segments) && pendingSegments.isEmpty()) {
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
