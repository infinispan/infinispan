package org.infinispan.util.concurrent;

import static org.infinispan.factories.KnownComponentNames.TIMEOUT_SCHEDULE_EXECUTOR;

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
import java.util.function.Supplier;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commons.util.Util;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.interceptors.distribution.Collector;
import org.infinispan.interceptors.distribution.PrimaryOwnerOnlyCollector;
import org.infinispan.remoting.transport.Address;
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

   private final ConcurrentHashMap<Long, BaseCollector<?>> collectorMap;
   private ScheduledExecutorService timeoutExecutor;

   public CommandAckCollector() {
      collectorMap = new ConcurrentHashMap<>();
   }

   @Inject
   public void inject(@ComponentName(TIMEOUT_SCHEDULE_EXECUTOR) ScheduledExecutorService timeoutExecutor) {
      this.timeoutExecutor = timeoutExecutor;
   }

   /**
    * Creates a collector for a single key write operation.
    *
    * @param cacheName    the cache name.
    * @param id           the id from {@link CommandInvocationId}.
    * @param topologyId   the current topology id.
    * @param timeout      the timeout value.
    * @param timeUnit     the timeout value's {@link TimeUnit}.
    * @param backupOwners the backup owners of the key.
    */
   public Collector<Object> create(String cacheName, long id, int topologyId, long timeout, TimeUnit timeUnit,
         Collection<Address> backupOwners) {
      if (backupOwners.isEmpty()) {
         return new PrimaryOwnerOnlyCollector<>();
      }
      SingleKeyCollector collector = new SingleKeyCollector(cacheName, id, topologyId, timeout, timeUnit, backupOwners);
      collectorMap.put(id, collector);
      if (trace) {
         log.tracef("Created new collector for %s. BackupOwners=%s", id, backupOwners);
      }
      return collector;
   }

   /**
    * Creates a collector for {@link org.infinispan.commands.write.PutMapCommand}.
    *
    * @param cacheName  the cache name.
    * @param id         the id from {@link CommandInvocationId#getId()}.
    * @param topologyId the current topology id.
    * @param timeout    the timeout value.
    * @param timeUnit   the timeout value's {@link TimeUnit}.
    * @param primary    a primary owners collection..
    * @param backups    a map between a backup owner and its segments affected.
    */
   public Collector<Map<Object, Object>> createMultiKeyCollector(String cacheName, long id, int topologyId,
         long timeout, TimeUnit timeUnit, Collection<Address> primary,
         Map<Address, Collection<Integer>> backups) {
      if (backups.isEmpty()) {
         return new PrimaryOwnerOnlyCollector<>();
      }
      MultiKeyCollector collector = new MultiKeyCollector(cacheName, id, topologyId, timeout, timeUnit,
            backups);
      collectorMap.put(id, collector);
      if (trace) {
         log.tracef("Created new collector for %s. Primary=%s. BackupSegments=%s", id, primary, backups);
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
      BaseCollector<?> collector = collectorMap.get(id);
      if (collector != null) {
         collector.completeExceptionally(throwable, topologyId);
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
   public boolean hasPendingBackupAcks(long id) {
      BaseCollector<?> collector = collectorMap.get(id);
      return collector != null && collector.hasPendingBackupAcks();
   }

   /**
    * Notifies a change in member list.
    *
    * @param members the new cluster members.
    */
   public void onMembersChange(String cacheName, Collection<Address> members) {
      Set<Address> currentMembers = new HashSet<>(members);
      collectorMap.values().stream().filter(collector -> collector.cacheName.equals(cacheName))
            .forEach(collector -> collector.onMembersChange(currentMembers));
   }

   private TimeoutException createTimeoutException(long id, long timeout, TimeUnit timeUnit) {
      return log.timeoutWaitingForAcks(Util.prettyPrintTime(timeout, timeUnit), id);
   }

   private abstract class BaseCollector<T> implements Callable<Void>, BiConsumer<T, Throwable>, Collector<T> {

      final long id;
      final CompletableFuture<T> future;
      final int topologyId;
      final String cacheName;
      final Supplier<TimeoutException> timeoutExceptionSupplier;
      private final ScheduledFuture<?> timeoutTask;
      volatile T primaryResult;
      volatile boolean primaryResultReceived = false;

      BaseCollector(String cacheName, long id, int topologyId, long timeout, TimeUnit timeUnit) {
         this.cacheName = cacheName;
         this.id = id;
         this.topologyId = topologyId;
         this.timeoutExceptionSupplier = () -> createTimeoutException(id, timeout, timeUnit);
         this.future = new CompletableFuture<>();
         this.timeoutTask = timeoutExecutor.schedule(this, timeout, timeUnit);
      }

      /**
       * Invoked by the timeout executor when the timeout expires.
       * <p>
       * It completes the future with the timeout exception.
       */
      @Override
      public final synchronized Void call() throws Exception {
         future.completeExceptionally(timeoutExceptionSupplier.get());
         return null;
      }

      /**
       * Invoked when the future is completed, it must cleanup all task related to this collector.
       * <p>
       * The tasks includes removing the collector from the map and cancel the timeout task.
       */
      @Override
      public final void accept(T t, Throwable throwable) {
         if (trace) {
            log.tracef("[Collector#%s] Collector completed with ret=%s, throw=%s", id, t, throwable);
         }
         collectorMap.remove(id);
         timeoutTask.cancel(false);
      }

      @Override
      public final CompletableFuture<T> getFuture() {
         return future.whenComplete(this);
      }

      @Override
      public void primaryException(Throwable throwable) {
         future.completeExceptionally(throwable);
      }

      final void completeExceptionally(Throwable throwable, int topologyId) {
         if (trace) {
            log.tracef(throwable, "[Collector#%s] completed exceptionally. TopologyId=%s (expected=%s)",
                  id, topologyId, this.topologyId);
         }
         if (isWrongTopologyOrIsDone(topologyId)) {
            return;
         }
         future.completeExceptionally(throwable);
      }

      abstract boolean hasPendingBackupAcks();

      abstract void onMembersChange(Collection<Address> members);

      final boolean isWrongTopologyOrIsDone(int topologyId) {
         return this.topologyId != topologyId || future.isDone();
      }
   }

   private class SingleKeyCollector extends BaseCollector<Object> {
      private final Collection<Address> owners;

      private SingleKeyCollector(String cacheName, long id, int topologyId, long timeout, TimeUnit timeUnit,
            Collection<Address> owners) {
         super(cacheName, id, topologyId, timeout, timeUnit);
         this.owners = Collections.synchronizedSet(new HashSet<>(owners)); //removal is fast
      }

      @Override
      public boolean hasPendingBackupAcks() {
         return !owners.isEmpty();
      }

      @Override
      public void onMembersChange(Collection<Address> members) {
         if (owners.retainAll(members) && owners.isEmpty() && primaryResultReceived) {
            if (trace) {
               log.tracef("[Collector#%s] Some backups left the cluster.", id);
            }
            markReady();
         }
      }

      @Override
      public void primaryResult(Object result, boolean success) {
         primaryResult = result;
         primaryResultReceived = true;
         if (!success || owners.isEmpty()) {
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
         if (owners.remove(from) && owners.isEmpty() && primaryResultReceived) {
            markReady();
         }
      }

      private void markReady() {
         if (trace) {
            log.tracef("[Collector#%s] Ready!", id);
         }
         future.complete(primaryResult);
      }
   }

   private class MultiKeyCollector extends BaseCollector<Map<Object, Object>> {
      @GuardedBy("this")
      private final Map<Address, Collection<Integer>> backups;

      MultiKeyCollector(String cacheName, long id, int topologyId, long timeout, TimeUnit timeUnit,
            Map<Address, Collection<Integer>> backups) {
         super(cacheName, id, topologyId, timeout, timeUnit);
         this.backups = backups;
      }

      @Override
      public synchronized boolean hasPendingBackupAcks() {
         return !backups.isEmpty();
      }

      @Override
      public synchronized void onMembersChange(Collection<Address> members) {
         if (backups.keySet().retainAll(members)) {
            if (trace) {
               log.tracef("[Collector#%s] Some backups left the cluster.", id);
            }
            checkCompleted();
         }
      }

      @Override
      public void primaryResult(Map<Object, Object> result, boolean success) {
         primaryResult = result;
         primaryResultReceived = true;
         synchronized (this) {
            checkCompleted();
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

      @GuardedBy("this")
      private void checkCompleted() {
         if (primaryResultReceived && backups.isEmpty()) {
            if (trace) {
               log.tracef("[Collector#%s] Ready! Return value=%ss.", id, primaryResult);
            }
            future.complete(primaryResult);
         }
      }
   }
}
