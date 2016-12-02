package org.infinispan.util.concurrent;

import org.infinispan.commands.CommandInvocationId;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

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

   public CommandAckCollector() {
      collectorMap = new ConcurrentHashMap<>();
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
    * <p>
    * If the collector can be cleanup after the {@link CompletableFuture} is completed, {@code cleanupAfterCompleted}
    * must be set to {@code true}.
    *
    * @param id                    the {@link CommandInvocationId}.
    * @param cleanupAfterCompleted if {@code true}, the collector is removed when the {@link CompletableFuture} is
    *                              completed.
    * @param <T>                   the type of the return value.
    * @return the collector's {@link CompletableFuture}.
    */
   public <T> CompletableFuture<T> getCollectorCompletableFuture(CommandInvocationId id,
         boolean cleanupAfterCompleted) {
      //noinspection unchecked
      Collector<T> collector = (Collector<T>) collectorMap.get(id);
      if (collector != null) {
         if (trace) {
            log.tracef("[Collector#%s] Waiting for acks asynchronously.", id);
         }
         CompletableFuture<T> future = collector.getFuture();
         if (cleanupAfterCompleted) {
            return future.whenComplete((t, throwable) -> collectorMap.remove(id));
         }
         return future;
      }
      return null;
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

   private interface Collector<T> {
      void completeExceptionally(Throwable throwable, int topologyId);

      boolean hasPendingBackupAcks();

      CompletableFuture<T> getFuture();

      void onMembersChange(Collection<Address> members);
   }

   private static class SingleKeyCollector implements Collector<Object> {
      private final CommandInvocationId id;
      private final CompletableFuture<Object> future;
      private final Collection<Address> owners;
      private final Address primaryOwner;
      private final int topologyId;
      private Object returnValue;

      private SingleKeyCollector(CommandInvocationId id, Collection<Address> owners, int topologyId) {
         this.id = id;
         this.primaryOwner = owners.iterator().next();
         this.topologyId = topologyId;
         this.future = new CompletableFuture<>();
         this.owners = new HashSet<>(owners); //removal is fast
      }

      private SingleKeyCollector(CommandInvocationId id, Object returnValue, Collection<Address> owners,
            int topologyId) {
         this.id = id;
         this.returnValue = returnValue;
         this.primaryOwner = owners.iterator().next();
         this.topologyId = topologyId;
         Collection<Address> tmpOwners = new HashSet<>(owners);
         tmpOwners.remove(primaryOwner);
         if (tmpOwners.isEmpty()) { //num owners is 1 or single member in cluster
            this.owners = Collections.emptyList();
            this.future = CompletableFuture.completedFuture(returnValue);
         } else {
            this.future = new CompletableFuture<>();
            this.owners = tmpOwners;
         }
      }

      @Override
      public synchronized void completeExceptionally(Throwable throwable, int topologyId) {
         if (trace) {
            log.tracef(throwable, "[Collector#%s] completed exceptionally. TopologyId=%s (expected=%s)",
                  id, topologyId, this.topologyId);
         }
         if (this.topologyId != topologyId) {
            return;
         }
         doCompleteExceptionally(throwable);
      }

      @Override
      public synchronized boolean hasPendingBackupAcks() {
         return owners.size() > 1 || //at least one backup + primary address
               //if one is missing, make sure that it isn't the primary
               owners.size() == 1 && !primaryOwner.equals(owners.iterator().next());
      }

      @Override
      public CompletableFuture<Object> getFuture() {
         return future;
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

      private void markReady() {
         if (trace) {
            log.tracef("[Collector#%s] Ready! Return value=%ss.", id, returnValue);
         }
         future.complete(returnValue);
      }

      private void doCompleteExceptionally(Throwable throwable) {
         owners.clear();
         future.completeExceptionally(throwable);
      }
   }

   private static class MultiKeyCollector implements Collector<Map<Object, Object>> {
      private final CommandInvocationId id;
      private final Map<Object, Object> returnValue;
      private final Collection<Address> primary;
      private final Map<Address, Collection<Integer>> backups;
      private final CompletableFuture<Map<Object, Object>> future;
      private final int topologyId;

      MultiKeyCollector(CommandInvocationId id, Collection<Address> primary, Map<Address, Collection<Integer>> backups,
            int topologyId) {
         this.id = id;
         this.topologyId = topologyId;
         this.returnValue = new HashMap<>();
         this.backups = backups;
         this.primary = new HashSet<>(primary);
         future = new CompletableFuture<>();
      }

      public synchronized void completeExceptionally(Throwable throwable, int topologyId) {
         if (trace) {
            log.tracef(throwable, "[Collector#%s] completed exceptionally. TopologyId=%s (expected=%s)",
                  id, topologyId, this.topologyId);
         }
         if (this.topologyId != topologyId) {
            return;
         }
         doCompleteExceptionally(throwable);
      }

      @Override
      public synchronized boolean hasPendingBackupAcks() {
         return !backups.isEmpty();
      }

      @Override
      public CompletableFuture<Map<Object, Object>> getFuture() {
         return future;
      }

      @Override
      public void onMembersChange(Collection<Address> members) {
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
         this.returnValue.putAll(returnValue);
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

      private void checkCompleted() {
         if (primary.isEmpty() && backups.isEmpty()) {
            if (trace) {
               log.tracef("[Collector#%s] Ready! Return value=%ss.", id, returnValue);
            }
            future.complete(returnValue);
         }
      }

      private void doCompleteExceptionally(Throwable throwable) {
         returnValue.clear();
         primary.clear();
         backups.clear();
         future.completeExceptionally(throwable);
      }


   }
}
