package org.infinispan.util.concurrent;

import static java.lang.String.format;

import java.util.ArrayList;
import java.util.Arrays;
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
import java.util.function.Function;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.interceptors.distribution.BiasedCollector;
import org.infinispan.interceptors.distribution.Collector;
import org.infinispan.interceptors.distribution.PrimaryOwnerOnlyCollector;
import org.infinispan.remoting.responses.ValidResponse;
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

   @Inject @ComponentName(KnownComponentNames.TIMEOUT_SCHEDULE_EXECUTOR)
   private ScheduledExecutorService timeoutExecutor;
   @Inject private Configuration configuration;

   private final ConcurrentHashMap<Long, BaseAckTarget> collectorMap;
   private long timeoutNanoSeconds;
   private Collection<Address> currentMembers;

   public CommandAckCollector() {
      collectorMap = new ConcurrentHashMap<>();
   }

   @Start
   public void start() {
      this.timeoutNanoSeconds = TimeUnit.MILLISECONDS.toNanos(configuration.clustering().remoteTimeout());
   }

   /**
    * Creates a collector for a single key write operation.
    * @param id           the id from {@link CommandInvocationId}.
    * @param backupOwners the backup owners of the key.
    * @param topologyId   the current topology id.
    */
   public <T> Collector<T> create(long id, Collection<Address> backupOwners, int topologyId) {
      if (backupOwners.isEmpty()) {
         return new PrimaryOwnerOnlyCollector<>();
      }
      SingleKeyCollector<T> collector = new SingleKeyCollector<>(id, backupOwners, topologyId);
      BaseAckTarget prev = collectorMap.put(id, collector);
      //is it possible the have a previous collector when the topology changes after the first collector is created
      //in that case, the previous collector must have a lower topology id
      assert prev == null || prev.topologyId < topologyId : format("replaced old collector '%s' by '%s'", prev, collector);
      if (trace) {
         log.tracef("Created new collector for %s. BackupOwners=%s", id, backupOwners);
      }
      return collector;
   }

   public BiasedCollector createBiased(long id, int topologyId) {
      BiasedKeyCollector collector = new BiasedKeyCollector(id, topologyId);
      BaseAckTarget prev = collectorMap.put(id, collector);
      assert prev == null || prev.topologyId < topologyId : prev.toString();
      if (trace) {
         log.tracef("Created new biased collector for %d", id);
      }
      return collector;
   }

   public MultiTargetCollector createMultiTargetCollector(long id, int primaries, int topologyId) {
      MultiTargetCollectorImpl multiTargetCollector = new MultiTargetCollectorImpl(id, primaries, topologyId);
      BaseAckTarget prev = collectorMap.put(id, multiTargetCollector);
      assert prev == null || prev.topologyId < topologyId : prev.toString();
      if (trace) {
         log.tracef("Created new multi target collector for %d", id);
      }
      return multiTargetCollector;
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
      BaseAckTarget prev = collectorMap.put(id, collector);
      //is it possible the have a previous collector when the topology changes after the first collector is created
      //in that case, the previous collector must have a lower topology id
      assert prev == null || prev.topologyId < topologyId : format("replaced old collector '%s' by '%s'", prev, collector);
      if (trace) {
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
   public void multiKeyBackupAck(long id, Address from, int segment, int topologyId) {
      SegmentBasedCollector collector = (SegmentBasedCollector) collectorMap.get(id);
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
      BaseAckTarget ackTarget = collectorMap.get(id);
      if (ackTarget instanceof SingleKeyCollector) {
         ((SingleKeyCollector) ackTarget).backupAck(topologyId, from);
      } else if (ackTarget instanceof MultiTargetCollectorImpl) {
         ((MultiTargetCollectorImpl) ackTarget).backupAck(topologyId, from);
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
      BaseAckTarget ackTarget = collectorMap.get(id);
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
      BaseAckTarget ackTarget = collectorMap.get(id);
      return ackTarget != null && ackTarget.hasPendingBackupAcks();
   }

   /**
    * Notifies a change in member list.
    *
    * @param members the new cluster members.
    */
   public void onMembersChange(Collection<Address> members) {
      Set<Address> currentMembers = new HashSet<>(members);
      this.currentMembers = currentMembers;
      for (BaseAckTarget<?> ackTarget : collectorMap.values()) {
         ackTarget.onMembersChange(currentMembers);
      }
   }

   private TimeoutException createTimeoutException(long id) {
      return log.timeoutWaitingForAcks(Util.prettyPrintTime(timeoutNanoSeconds, TimeUnit.NANOSECONDS), id);
   }

   private abstract class BaseAckTarget<T> implements Callable<Void>, BiConsumer<T, Throwable> {
      final long id;
      final int topologyId;
      final ScheduledFuture<?> timeoutTask;

      private BaseAckTarget(long id, int topologyId) {
         this.topologyId = topologyId;
         this.id = id;
         this.timeoutTask = timeoutExecutor.schedule(this, timeoutNanoSeconds, TimeUnit.NANOSECONDS);
      }

      /**
       * Invoked when the collector's future is completed, it must cleanup all task related to this collector.
       * <p>
       * The tasks includes removing the collector from the map and cancel the timeout task.
       */
      public final void accept(T t, Throwable throwable) {
         if (trace) {
            log.tracef("[Collector#%s] Collector completed with ret=%s, throw=%s", id, t, throwable);
         }
         boolean removed = collectorMap.remove(id, this);
         assert removed;
         timeoutTask.cancel(false);
      }

      abstract void completeExceptionally(Throwable throwable, int topologyId);
      abstract boolean hasPendingBackupAcks();
      abstract void onMembersChange(Collection<Address> members);
   }

   private abstract class BaseCollector<T> extends BaseAckTarget<T> implements Collector<T> {

      final CompletableFuture<T> future;
      final CompletableFuture<T> exposedFuture;
      volatile T primaryResult;
      volatile boolean primaryResultReceived = false;

      BaseCollector(long id, int topologyId) {
         super(id, topologyId);
         this.future = new CompletableFuture<>();
         this.exposedFuture = future.whenComplete(this);
      }

      /**
       * Invoked by the timeout executor when the timeout expires.
       * <p>
       * It completes the future with the timeout exception.
       */
      @Override
      public final synchronized Void call() {
         future.completeExceptionally(createTimeoutException(id));
         return null;
      }

      @Override
      public final CompletableFuture<T> getFuture() {
         return exposedFuture;
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

      final boolean isWrongTopologyOrIsDone(int topologyId) {
         return this.topologyId != topologyId || future.isDone();
      }
   }

   private class SingleKeyCollector<T> extends BaseCollector<T> {
      final Collection<Address> backupOwners;

      private SingleKeyCollector(long id, Collection<Address> backupOwners, int topologyId) {
         super(id, topologyId);
         this.backupOwners = new HashSet<>(backupOwners);
      }

      @Override
      synchronized boolean hasPendingBackupAcks() {
         if (trace) {
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
            if (trace) {
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

      void backupAck(int topologyId, Address from) {
         if (trace) {
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
         if (trace) {
            log.tracef("[Collector#%s] Ready!", id);
         }
         future.complete(primaryResult);
      }
   }

   private class BiasedKeyCollector extends SingleKeyCollector<ValidResponse> implements BiasedCollector {
      @SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
      private Collection<Address> unsolicitedAcks;

      private BiasedKeyCollector(long id, int topologyId) {
         super(id, Collections.emptyList(), topologyId);
      }

      void backupAck(int topologyId, Address from) {
         if (trace) {
            log.tracef("[Collector#%s] Backup ACK. Address=%s, TopologyId=%s (expected=%s)",
                  id, from, topologyId, this.topologyId);
         }
         if (isWrongTopologyOrIsDone(topologyId)) {
            return;
         }
         boolean empty;
         synchronized (this) {
            if (!backupOwners.remove(from)) {
               if (unsolicitedAcks == null) {
                  unsolicitedAcks = new ArrayList<>(4);
               }
               log.tracef("[Collector#%s] Unsolicited ACK", id);
               unsolicitedAcks.add(from);
            }
            empty = backupOwners.isEmpty();
         }
         if (empty && primaryResultReceived) {
            markReady();
         }
      }

      @Override
      public synchronized void addPendingAcks(boolean success, Address[] waitFor) {
         if (success && waitFor != null) {
            Collection<Address> members = currentMembers;
            for (Address address : waitFor) {
               if (members == null || members.contains(address)) {
                  backupOwners.add(address);
               }
            }
         }
         if (unsolicitedAcks != null) {
            unsolicitedAcks.removeIf(backupOwners::remove);
         }
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
            if (trace) {
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

      @Override
      public String toString() {
         final StringBuilder sb = new StringBuilder("SegmentBasedCollector{");
         sb.append("id=").append(id);
         sb.append(", topologyId=").append(topologyId);
         sb.append(", primaryResult=").append(primaryResult);
         sb.append(", primaryResultReceived=").append(primaryResultReceived);
         sb.append(", backups=").append(backups);
         sb.append('}');
         return sb.toString();
      }
   }

   /**
    * Contrary to {@link MultiTargetCollectorImpl} implements the {@link Collector} interface delegating its calls
    * to the {@link MultiTargetCollectorImpl} which is stored in {@link #collectorMap}.
    */
   private static class SingleTargetCollectorImpl implements BiasedCollector, Function<Void, CompletableFuture<ValidResponse>> {
      private final MultiTargetCollectorImpl parent;
      private final CompletableFuture<ValidResponse> resultFuture = new CompletableFuture<>();
      private final CompletableFuture<ValidResponse> combinedFuture;

      private SingleTargetCollectorImpl(MultiTargetCollectorImpl parent) {
         this.parent = parent;
         this.combinedFuture = CompletableFuture.allOf(resultFuture, parent.acksFuture).thenCompose(this);
      }

      @Override
      public CompletableFuture<ValidResponse> getFuture() {
         return combinedFuture;
      }

      @Override
      public void primaryException(Throwable throwable) {
         // exceptions can propagate immediately
         combinedFuture.completeExceptionally(throwable);
      }

      @Override
      public void primaryResult(ValidResponse result, boolean success) {
         if (trace) {
            log.tracef("Received result for %d, topology %d: %s", parent.id, parent.topologyId, result);
         }
         resultFuture.complete(result);
         parent.checkComplete();
      }

      @Override
      public CompletableFuture<ValidResponse> apply(Void nil) {
         return resultFuture;
      }

      @Override
      public void addPendingAcks(boolean success, Address[] waitFor) {
         if (success && waitFor != null) {
            parent.addPendingAcks(waitFor);
         }
      }
   }

   public interface MultiTargetCollector {
      BiasedCollector collectorFor(Address target);
   }

   private class MultiTargetCollectorImpl extends BaseAckTarget<Void> implements MultiTargetCollector {
      private final Map<Address, SingleTargetCollectorImpl> primaryCollectors = new HashMap<>();
      // Note that this is a list, since we may expect multiple acks from single node.
      private final List<Address> pendingAcks = new ArrayList<>();
      private final CompletableFuture<Void> acksFuture = new CompletableFuture<>();
      private final int primaries;
      @SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
      private List<Address> unsolicitedAcks;
      private Throwable throwable;

      MultiTargetCollectorImpl(long id, int primaries, int topologyId) {
         super(id, topologyId);
         this.primaries = primaries;
         acksFuture.whenComplete(this);
      }

      @Override
      public synchronized BiasedCollector collectorFor(Address target) {
         if (throwable != null) {
            throw CompletableFutures.asCompletionException(throwable);
         } else {
            SingleTargetCollectorImpl collector = new SingleTargetCollectorImpl(this);
            Collector<ValidResponse> prev = primaryCollectors.put(target, collector);
            assert prev == null : prev.toString();
            return collector;
         }
      }

      synchronized void addPendingAcks(Address[] waitFor) {
         if (trace) {
            log.tracef("[Collector#%s] Adding pending acks from %s, existing are %s",
                  id, Arrays.toString(waitFor), pendingAcks);
         }
         Collection<Address> members = currentMembers;
         for (Address member : waitFor) {
            if (members == null || members.contains(member)) {
               pendingAcks.add(member);
            }
         }
         if (unsolicitedAcks != null) {
            // this should work for multiple acks from same node as well
            unsolicitedAcks.removeIf(pendingAcks::remove);
         }
      }

      synchronized void backupAck(int topologyId, Address from) {
         if (trace) {
            log.tracef("[Collector#%s] PutMap Backup ACK. Address=%s. TopologyId=%s (expected=%s).",
                  id, from, topologyId, this.topologyId);
         }
         if (topologyId == this.topologyId) {
            if (!pendingAcks.remove(from)) {
               if (unsolicitedAcks == null) {
                  unsolicitedAcks = new ArrayList<>(4);
               }
               unsolicitedAcks.add(from);
            }
         }
         checkComplete();
      }

      @Override
      synchronized void completeExceptionally(Throwable throwable, int topologyId) {
         if (topologyId == this.topologyId) {
            this.throwable = throwable;
            for (Collector<?> collector : primaryCollectors.values()) {
               collector.primaryException(throwable);
            }
         }
      }

      @Override
      synchronized boolean hasPendingBackupAcks() {
         return !pendingAcks.isEmpty();
      }

      @Override
      synchronized void onMembersChange(Collection<Address> members) {
         pendingAcks.retainAll(members);
         for (Map.Entry<Address, SingleTargetCollectorImpl> pair : primaryCollectors.entrySet()) {
            if (!members.contains(pair.getKey())) {
               pair.getValue().primaryException(OutdatedTopologyException.INSTANCE);
            }
         }
      }

      @Override
      public Void call() {
         completeExceptionally(createTimeoutException(id), topologyId);
         return null;
      }

      synchronized void checkComplete() {
         if (primaries != primaryCollectors.size()) {
            return;
         }
         for (SingleTargetCollectorImpl c : primaryCollectors.values()) {
            if (!c.resultFuture.isDone()) return;
         }
         if (!hasPendingBackupAcks()) {
            acksFuture.complete(null);
         }
      }
   }
}
