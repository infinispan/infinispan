package org.infinispan.xsite.irac;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.infinispan.commons.time.TimeService;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Maybe;
import io.reactivex.rxjava3.core.Scheduler;
import io.reactivex.rxjava3.core.Single;
import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.schedulers.Schedulers;

/**
 * Removes queued keys that have neither an entry nor a tombstone, so they can never be sent to the remote site(s).
 * <p>
 * Safety over speed: a key is removed only if it is older than the minimum age, this node is its primary owner, and
 * it has neither an entry (in memory or in a store) nor a tombstone in two checks separated by a grace period. Between
 * the checks, the key must not be updated: a write replaces the queued state, so the {@link IracManagerKeyInfo} is
 * compared. The keys are checked one at a time, in batches with a pause between them, so the cleanup does not compete
 * with application traffic.
 *
 * @since 16.4
 */
class IracQueueCleanup {

   private static final Log log = LogFactory.getLog(IracQueueCleanup.class);

   enum State {IDLE, RUNNING, COMPLETED, CANCELLED, FAILED}

   /**
    * The view of the IRAC queue used by the cleanup.
    */
   interface Queue {
      String cacheName();

      Collection<IracManagerKeyState> snapshot();

      IracManagerKeyState current(Object key);

      boolean isPrimary(int segment);

      /**
       * @return {@code true} if the key has neither an entry, in memory or in a store, nor a tombstone.
       */
      CompletionStage<Boolean> isMissing(IracManagerKeyState state);

      void remove(Collection<IracManagerKeyState> states);
   }

   record Settings(long minAgeMillis, long gracePeriodMillis, int batchSize, long pauseMillis) {
      static final long DEFAULT_GRACE_PERIOD_MILLIS = TimeUnit.SECONDS.toMillis(30);
      static final int DEFAULT_BATCH_SIZE = 100;
      static final long DEFAULT_PAUSE_MILLIS = 100;

      Settings {
         if (minAgeMillis < 0 || gracePeriodMillis < 0 || batchSize <= 0 || pauseMillis < 0) {
            throw new IllegalArgumentException("Invalid queue cleanup settings: " + minAgeMillis + ", " +
                  gracePeriodMillis + ", " + batchSize + ", " + pauseMillis);
         }
      }

      Settings withMinAge(long minAgeMillis) {
         return new Settings(minAgeMillis, gracePeriodMillis, batchSize, pauseMillis);
      }
   }

   private final Queue queue;
   private final TimeService timeService;
   private final Scheduler scheduler;
   private final AtomicLong checked = new AtomicLong();
   private final AtomicLong candidates = new AtomicLong();
   private final AtomicLong removed = new AtomicLong();
   private volatile State state = State.IDLE;
   private Disposable running;

   IracQueueCleanup(Queue queue, TimeService timeService, ScheduledExecutorService scheduledExecutor) {
      this.queue = queue;
      this.timeService = timeService;
      this.scheduler = Schedulers.from(scheduledExecutor);
   }

   /**
    * Starts the cleanup, unless it is already running.
    *
    * @return The status after the start.
    */
   synchronized IracQueueCleanupStatus start(Settings settings) {
      if (state == State.RUNNING) {
         return status();
      }
      checked.set(0);
      candidates.set(0);
      removed.set(0);
      state = State.RUNNING;
      running = firstCheck(settings)
            .delay(settings.gracePeriodMillis(), TimeUnit.MILLISECONDS, scheduler)
            .flatMapCompletable(keys -> secondCheck(keys, settings))
            .subscribe(this::onCompleted, this::onFailed);
      return status();
   }

   synchronized IracQueueCleanupStatus cancel() {
      if (state == State.RUNNING) {
         running.dispose();
         state = State.CANCELLED;
      }
      return status();
   }

   IracQueueCleanupStatus status() {
      Collection<IracManagerKeyState> snapshot = queue.snapshot();
      long oldestAge = 0;
      for (IracManagerKeyState s : snapshot) {
         oldestAge = Math.max(oldestAge, ageMillis(s));
      }
      return new IracQueueCleanupStatus(state.name(), snapshot.size(), oldestAge, checked.get(), candidates.get(),
            removed.get());
   }

   private Single<List<IracManagerKeyInfo>> firstCheck(Settings settings) {
      return Flowable.fromIterable(queue.snapshot())
            .filter(s -> ageMillis(s) >= settings.minAgeMillis() && queue.isPrimary(s.getSegment()))
            .buffer(settings.batchSize())
            .concatMapSingle(batch -> Flowable.fromIterable(batch)
                  .concatMapMaybe(this::firstCheck)
                  .toList()
                  .delay(settings.pauseMillis(), TimeUnit.MILLISECONDS, scheduler))
            .flatMapIterable(keys -> keys)
            .toList();
   }

   private Maybe<IracManagerKeyInfo> firstCheck(IracManagerKeyState keyState) {
      checked.incrementAndGet();
      return isMissing(keyState)
            .doOnSuccess(ignore -> candidates.incrementAndGet())
            .map(ignore -> keyState.getKeyInfo());
   }

   private Completable secondCheck(List<IracManagerKeyInfo> keys, Settings settings) {
      return Flowable.fromIterable(keys)
            .buffer(settings.batchSize())
            .concatMapCompletable(batch -> Flowable.fromIterable(batch)
                  .concatMapMaybe(this::secondCheck)
                  .toList()
                  .doOnSuccess(this::remove)
                  .ignoreElement()
                  .delay(settings.pauseMillis(), TimeUnit.MILLISECONDS, scheduler));
   }

   private Maybe<IracManagerKeyState> secondCheck(IracManagerKeyInfo keyInfo) {
      IracManagerKeyState current = queue.current(keyInfo.getKey());
      if (!isUnchanged(keyInfo, current)) {
         return Maybe.empty();
      }
      return isMissing(current).map(ignore -> current);
   }

   private boolean isUnchanged(IracManagerKeyInfo keyInfo, IracManagerKeyState current) {
      // a write in between replaces the state, and the new update must be sent.
      boolean sameState = current != null && keyInfo.equals(current.getKeyInfo());
      return sameState && queue.isPrimary(keyInfo.getSegment());
   }

   private Maybe<Boolean> isMissing(IracManagerKeyState keyState) {
      return Maybe.fromCompletionStage(queue.isMissing(keyState))
            .filter(Boolean::booleanValue)
            .onErrorComplete(t -> {
               // a failure to load does not mean the entry is missing; keep the key.
               log.debugf(t, "[IRAC] Queue cleanup failed to load key %s. The key is kept.", keyState.getKeyInfo());
               return true;
            });
   }

   private void remove(List<IracManagerKeyState> states) {
      if (states.isEmpty()) {
         return;
      }
      queue.remove(states);
      removed.addAndGet(states.size());
   }

   private synchronized void onCompleted() {
      state = State.COMPLETED;
      log.iracQueueCleanupCompleted(queue.cacheName(), checked.get(), removed.get());
   }

   private synchronized void onFailed(Throwable throwable) {
      state = State.FAILED;
      log.unexpectedErrorFromIrac(throwable);
   }

   private long ageMillis(IracManagerKeyState keyState) {
      return timeService.timeDuration(keyState.getTrackedAt(), TimeUnit.MILLISECONDS);
   }
}
