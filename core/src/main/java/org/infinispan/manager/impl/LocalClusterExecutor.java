package org.infinispan.manager.impl;

import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;

import javax.security.auth.Subject;

import org.infinispan.manager.ClusterExecutionPolicy;
import org.infinispan.manager.ClusterExecutor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.security.Security;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.function.TriConsumer;

/**
 * @author wburns
 * @since 9.0
 */
class LocalClusterExecutor implements ClusterExecutor {
   protected final Predicate<? super Address> predicate;
   protected final EmbeddedCacheManager manager;
   protected final long time;
   protected final TimeUnit unit;
   protected final Executor localExecutor;
   protected final ScheduledExecutorService timeoutExecutor;

   LocalClusterExecutor(Predicate<? super Address> predicate, EmbeddedCacheManager manager, Executor localExecutor,
         long time, TimeUnit unit, ScheduledExecutorService timeoutExecutor) {
      this.predicate = predicate;
      this.manager = new UnwrappingEmbeddedCacheManager(Objects.requireNonNull(manager));
      this.localExecutor = Objects.requireNonNull(localExecutor);
      if (time <= 0) {
         throw new IllegalArgumentException("time must be greater than 0");
      }
      this.time = time;
      this.unit = Objects.requireNonNull(unit);
      this.timeoutExecutor = Objects.requireNonNull(timeoutExecutor);
   }

   Address getMyAddress() {
      return null;
   }

   @Override
   public void execute(Runnable command) {
      // We ignore time out since user can't ever even respond to it, so no reason to create extra fluff
      localExecutor.execute(command);
   }

   @Override
   public CompletableFuture<Void> submit(Runnable command) {
      CompletableFuture<Void> future = new CompletableFuture<>();
      localExecutor.execute(() -> {
         try {
            command.run();
            future.complete(null);
         } catch (Throwable t) {
            future.completeExceptionally(t);
         }
      });
      ScheduledFuture<Boolean> scheduledFuture = timeoutExecutor.schedule(
            () -> future.completeExceptionally(new TimeoutException()), time, unit);
      future.whenComplete((v, t) -> scheduledFuture.cancel(true));
      return future;
   }

   @Override
   public <V> CompletableFuture<Void> submitConsumer(Function<? super EmbeddedCacheManager, ? extends V> callable,
         TriConsumer<? super Address, ? super V, ? super Throwable> triConsumer) {
      CompletableFuture<Void> future = new CompletableFuture<>();
      localInvocation(callable).whenComplete((r, t) -> {
         try {
            triConsumer.accept(getMyAddress(), r, t);
            future.complete(null);
         } catch (Throwable throwable) {
            future.completeExceptionally(throwable);
         }
      });
      ScheduledFuture<Boolean> scheduledFuture = timeoutExecutor.schedule(() ->
            future.completeExceptionally(new TimeoutException()), time, unit);
      future.whenComplete((v, t) -> scheduledFuture.cancel(true));
      return future;
   }

   <T> CompletableFuture<T> localInvocation(Function<? super EmbeddedCacheManager, ? extends T> function) {
      CompletableFuture<T> future = new CompletableFuture<>();
      Subject subject = Security.getSubject();
      localExecutor.execute(() -> {
         try {
            T result = Security.doAs(subject, function, manager);
            future.complete(result);
         } catch (Throwable t) {
            future.completeExceptionally(t);
         }
      });
      return future;
   }

   protected ClusterExecutor sameClusterExecutor(Predicate<? super Address> predicate,
         long time, TimeUnit unit) {
      return new LocalClusterExecutor(predicate, manager, localExecutor, time, unit, timeoutExecutor);
   }

   @Override
   public ClusterExecutor timeout(long time, TimeUnit unit) {
      if (time <= 0) {
         throw new IllegalArgumentException("Time must be greater than 0!");
      }
      Objects.requireNonNull(unit, "TimeUnit must be non null!");
      if (this.time == time && this.unit == unit) {
         return this;
      }
      return sameClusterExecutor(predicate, time, unit);
   }

   @Override
   public ClusterExecutor filterTargets(Predicate<? super Address> predicate) {
      return sameClusterExecutor(predicate, time, unit);
   }

   @Override
   public ClusterExecutor filterTargets(ClusterExecutionPolicy policy) throws IllegalStateException {
      throw new IllegalStateException();
   }

   @Override
   public ClusterExecutor filterTargets(ClusterExecutionPolicy policy, Predicate<? super Address> predicate) throws IllegalStateException {
      throw new IllegalStateException();
   }

   @Override
   public ClusterExecutor filterTargets(Collection<Address> addresses) {
      return filterTargets(addresses::contains);
   }

   @Override
   public ClusterExecutor noFilter() {
      if (predicate == null) {
         return this;
      }
      return sameClusterExecutor(null, time, unit);
   }

   @Override
   public ClusterExecutor singleNodeSubmission() {
      return this;
   }

   @Override
   public ClusterExecutor singleNodeSubmission(int failOverCount) {
      return new FailOverClusterExecutor(this, failOverCount);
   }

   @Override
   public ClusterExecutor allNodeSubmission() {
      return this;
   }
}
