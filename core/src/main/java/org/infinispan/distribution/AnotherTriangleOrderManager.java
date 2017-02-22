package org.infinispan.distribution;

import static org.infinispan.factories.KnownComponentNames.TIMEOUT_SCHEDULE_EXECUTOR;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;

/**
 * //TODO document this!
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public class AnotherTriangleOrderManager implements Callable<Void> {

   private final ConcurrentHashMap<Object, ExtendedCommandPosition> order;
   private ScheduledExecutorService executorService;
   private volatile ScheduledFuture<Void> cleanupFuture;


   public AnotherTriangleOrderManager() {
      order = new ConcurrentHashMap<>();
   }

   @Inject
   public void inject(@ComponentName(TIMEOUT_SCHEDULE_EXECUTOR) ScheduledExecutorService executorService) {
      this.executorService = executorService;
   }

   @Start
   public void start() {
      if (cleanupFuture == null) {
         cleanupFuture = executorService.schedule(this, 30, TimeUnit.SECONDS);
      }
   }

   @Stop
   public void stop() {
      cleanupFuture.cancel(false);
      cleanupFuture = null;
   }

   public CommandPosition orderKey(Object key) {
      SingleKeyCommandPosition position = new SingleKeyCommandPosition();
      position.dependency = order.put(key, position);
      return position;
   }

   public CommandPosition orderMultipleKeys(Set<Object> keys) {
      Iterator<Object> iterator = keys.iterator();
      int size = keys.size();
      if (size == 1) {
         return orderKey(iterator.next());
      }
      MultiKeyCommandPosition position = new MultiKeyCommandPosition(size);
      while (iterator.hasNext()) {
         ExtendedCommandPosition dependency = order.put(iterator.next(), position);
         position.dependencies.add(dependency);
      }
      return position;
   }

   @Override
   public Void call() throws Exception {
      cleanup();
      return null;
   }

   private void cleanup() {
      //there is a better way to do it?
      //iterator.remove() wouldn't work because a thread may have put a new value
      Map<Object, ExtendedCommandPosition> toRemove = new HashMap<>();
      for (Map.Entry<Object, ExtendedCommandPosition> entry : order.entrySet()) {
         if (entry.getValue().hasFinished()) {
            toRemove.put(entry.getKey(), entry.getValue());
         }
      }
      toRemove.entrySet().forEach(entry -> order.remove(entry.getKey(), entry.getValue()));
   }

   private interface ExtendedCommandPosition extends CommandPosition {
      boolean hasFinished();
   }

   private static class MultiKeyCommandPosition implements ExtendedCommandPosition {

      private final List<ExtendedCommandPosition> dependencies;
      private volatile boolean finished;

      private MultiKeyCommandPosition(int size) {
         dependencies = Collections.synchronizedList(new ArrayList<>(size));
      }

      @Override
      public boolean isNext() {
         for (ExtendedCommandPosition dependency : dependencies) {
            if (!dependency.hasFinished()) {
               return false;
            }
         }
         return true;
      }

      @Override
      public void finish() {
         dependencies.clear();
         finished = true;
      }

      @Override
      public boolean hasFinished() {
         return finished;
      }
   }

   private static class SingleKeyCommandPosition implements ExtendedCommandPosition {

      private volatile ExtendedCommandPosition dependency;
      private volatile boolean finished;

      @Override
      public boolean isNext() {
         return dependency == null || dependency.hasFinished();
      }

      @Override
      public void finish() {
         dependency = null; //for GC
         finished = true;
      }

      @Override
      public boolean hasFinished() {
         return finished;
      }
   }

}
