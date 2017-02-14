package org.infinispan.util.concurrent;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.Configuration;
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
@Scope(Scopes.NAMED_CACHE)
public class CommandAckCollector {

   private static final Log log = LogFactory.getLog(CommandAckCollector.class);
   private static final boolean trace = log.isTraceEnabled();

   private final ConcurrentHashMap<Long, Collector<?>> collectorMap;
   private ScheduledExecutorService timeoutExecutor;
   private long timeoutNanoSeconds;

   public CommandAckCollector() {
      collectorMap = new ConcurrentHashMap<>();
   }

   private static TimeoutException createTimeoutException(long timeoutNanoSeconds, long id) {
      return log.timeoutWaitingForAcks(Util.prettyPrintTime(timeoutNanoSeconds, TimeUnit.NANOSECONDS), id);
   }

   @Inject
   public void inject(
         @ComponentName(KnownComponentNames.TIMEOUT_SCHEDULE_EXECUTOR) ScheduledExecutorService timeoutExecutor,
         Configuration configuration) {
      this.timeoutExecutor = timeoutExecutor;
      this.timeoutNanoSeconds = TimeUnit.MILLISECONDS.toNanos(configuration.clustering().remoteTimeout());
   }

   /**
    * Creates a collector for a single key write operation.
    *
    * @param id         the id from {@link CommandInvocationId#getId()}.
    * @param owners     the owners of the key. It assumes the first element as primary owner.
    * @param topologyId the current topology id.
    */
   public void create(long id, Collection<Address> owners, int topologyId) {
      SingleKeyCollector collector = new SingleKeyCollector(id, owners, topologyId);
      collector.setCleanupTask();
      collectorMap.put(id, collector);
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
    * @param id           the id from {@link CommandInvocationId#getId()}.
    * @param returnValue  the primary owner result.
    * @param primaryOwner the primary owner address
    * @param backupOwners the backup owners of the key.
    * @param topologyId   the current topology id.
    */
   public void create(long id, Object returnValue, Address primaryOwner, Collection<Address> backupOwners,
         int topologyId) {
      SingleKeyCollector collector = new SingleKeyCollector(id, returnValue, primaryOwner, backupOwners, topologyId);
      collector.setCleanupTask();
      collectorMap.put(id, collector);
      if (trace) {
         log.tracef("Created new collector for %s. ReturnValue=%s. Owners=%s", id, returnValue, backupOwners);
      }
   }

   /**
    * Creates a collector for {@link org.infinispan.commands.write.PutMapCommand}.
    *
    * @param id         the id from {@link CommandInvocationId#getId()}.
    * @param primary    a primary owners collection..
    * @param backups    a map between a backup owner and its segments affected.
    * @param topologyId the current topology id.
    */
   public void createMultiKeyCollector(long id, Collection<Address> primary, Map<Address, Collection<Integer>> backups,
         int topologyId) {
      MultiKeyCollector collector = new MultiKeyCollector(id, primary, backups, topologyId);
      collector.setCleanupTask();
      collectorMap.put(id, collector);
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
    * Acknowledges a write operation completion in the primary owner.
    * <p>
    * If the operation does not succeed (conditional commands), the collector is completed without waiting for the
    * acknowledges from the backup owners.
    *
    * @param id          the id from {@link CommandInvocationId#getId()}.
    * @param returnValue the return value.
    * @param success     {@code true} if the operation succeed in the primary owner, {@code false} otherwise.
    * @param from        the primary owner.
    * @param topologyId  the topology id.
    */
   public void primaryAck(long id, Object returnValue, boolean success, Address from, int topologyId) {
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

   private abstract class Collector<T> implements BiConsumer<T, Throwable> {

      final long id;
      final CompletableFuture<T> future;
      final int topologyId;
      private volatile ScheduledFuture<?> timeoutTask;

      Collector(long id, int topologyId) {
         this.id = id;
         this.topologyId = topologyId;
         this.future = new CompletableFuture<>();
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

      final void completeExceptionally(Throwable throwable, int topologyId) {
         if (trace) {
            log.tracef(throwable, "[Collector#%s] completed exceptionally. TopologyId=%s (expected=%s)",
                  (Object) id, topologyId, this.topologyId);
         }
         if (isWrongTopologyOrIsDone(topologyId)) {
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
         return future.whenComplete(this);
      }

      final void setCleanupTask() {
         this.timeoutTask = timeoutExecutor
               .schedule(() -> doCompleteExceptionally(createTimeoutException(timeoutNanoSeconds, id)),
                     timeoutNanoSeconds, TimeUnit.NANOSECONDS);
      }

      final boolean isWrongTopologyOrIsDone(int topologyId) {
         return this.topologyId != topologyId || future.isDone();
      }
   }

   private class SingleKeyCollector extends Collector<Object> {
      @GuardedBy("owners")
      private final HashSet<Address> owners;
      private final Address primaryOwner;
      private volatile Object returnValue;

      private SingleKeyCollector(long id, Collection<Address> owners, int topologyId) {
         super(id, topologyId);
         this.primaryOwner = owners.iterator().next();
         this.owners = new HashSet<>(owners); //removal is fast
      }

      private SingleKeyCollector(long id, Object returnValue, Address primaryOwner, Collection<Address> owners,
            int topologyId) {
         super(id, topologyId);
         this.returnValue = returnValue;
         this.primaryOwner = primaryOwner;
         this.owners = new HashSet<>(owners);
      }

      @Override
      public boolean hasPendingBackupAcks() {
         synchronized (owners) {
            return owners.size() > 1 || //at least one backup + primary address
                  //if one is missing, make sure that it isn't the primary
                  owners.size() == 1 && !owners.iterator().next().equals(primaryOwner);
         }
      }

      @Override
      public void onMembersChange(Collection<Address> members) {
         if (!members.contains(primaryOwner)) {
            //primary owner left. throw OutdatedTopologyException to trigger a retry
            if (trace) {
               log.tracef("[Collector#%s] The Primary Owner left the cluster.", id);
            }
            doCompleteExceptionally(OutdatedTopologyException.getCachedInstance());
         } else if (haveAllBackupsLeft(members)) {
            if (trace) {
               log.tracef("[Collector#%s] Some backups left the cluster.", id);
            }
            markReady();
         }
      }

      void primaryAck(int topologyId, Object returnValue, boolean success, Address from) {
         if (trace) {
            log.tracef(
                  "[Collector#%s] Primary ACK. Success=%s. ReturnValue=%s. Address=%s, TopologyId=%s (expected=%s)",
                  id, success, returnValue, from, topologyId, this.topologyId);
         }
         if (isWrongTopologyOrIsDone(topologyId)) {
            return;
         }
         if (checkPrimaryAck(returnValue, from) || !success) {
            markReady();
         }
      }

      void backupAck(int topologyId, Address from) {
         if (trace) {
            log.tracef("[Collector#%s] Backup ACK. Address=%s, TopologyId=%s (expected=%s)",
                  id, from, topologyId, this.topologyId);
         }
         if (isWrongTopologyOrIsDone(topologyId)) {
            return;
         }
         if (removeBackupOwner(from)) {
            markReady();
         }
      }

      void doCompleteExceptionally(Throwable throwable) {
         future.completeExceptionally(throwable);
      }

      //return true if the primary ack was the last, false otherwise.
      private boolean checkPrimaryAck(Object returnValue, Address from) {
         synchronized (owners) {
            if (owners.remove(from)) {
               this.returnValue = returnValue;
               return owners.isEmpty();
            }
            return false;
         }
      }

      private boolean haveAllBackupsLeft(Collection<Address> members) {
         synchronized (owners) {
            return owners.retainAll(members) && owners.isEmpty();
         }
      }

      private boolean removeBackupOwner(Address member) {
         synchronized (owners) {
            return owners.remove(member) && owners.isEmpty();
         }
      }

      private void markReady() {
         if (trace) {
            log.tracef("[Collector#%s] Ready! Return value=%ss.", id, returnValue);
         }
         future.complete(returnValue);
      }
   }

   private class MultiKeyCollector extends Collector<Map<Object, Object>> {
      @GuardedBy("this")
      private final HashSet<Address> primary;
      @GuardedBy("this")
      private final HashMap<Address, Collection<Integer>> backups;
      @GuardedBy("this")
      private HashMap<Object, Object> returnValue;

      MultiKeyCollector(long id, Collection<Address> primary, Map<Address, Collection<Integer>> backups,
            int topologyId) {
         super(id, topologyId);
         this.returnValue = null;
         this.backups = new HashMap<>(backups);
         this.primary = new HashSet<>(primary);
      }

      @Override
      public synchronized boolean hasPendingBackupAcks() {
         return !backups.isEmpty();
      }

      @Override
      public void onMembersChange(Collection<Address> members) {
         if (hasSomePrimaryLeft(members)) {
            //primary owner left. throw OutdatedTopologyException to trigger a retry
            if (trace) {
               log.tracef("[Collector#%s] A primary Owner left the cluster.", id);
            }
            doCompleteExceptionally(OutdatedTopologyException.getCachedInstance());
         } else if (hasSomeBackupLeft(members)) {
            if (trace) {
               log.tracef("[Collector#%s] Some backups left the cluster.", id);
            }
            checkCompleted();
         }
      }

      void primaryAck(Map<Object, Object> returnValue, Address from, int topologyId) {
         if (trace) {
            log.tracef("[Collector#%s] PutMap Primary ACK. Address=%s. TopologyId=%s (expected=%s)",
                  id, from, topologyId, this.topologyId);
         }
         if (isWrongTopologyOrIsDone(topologyId)) {
            return;
         }
         synchronized (this) {
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
      }

      void backupAck(Address from, int segment, int topologyId) {
         if (trace) {
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
            }
            checkCompleted();
         }
      }

      void doCompleteExceptionally(Throwable throwable) {
         future.completeExceptionally(throwable);
      }

      private synchronized boolean hasSomePrimaryLeft(Collection<Address> members) {
         return !members.containsAll(primary);

      }

      private synchronized boolean hasSomeBackupLeft(Collection<Address> members) {
         return backups.keySet().retainAll(members);
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
