package org.infinispan.server.resp.scripting;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.infinispan.commons.util.Util;

public class LuaContextPool {
   private final ConcurrentLinkedQueue<LuaContext> pool;
   private final ScheduledExecutorService executorService;
   private final Supplier<LuaContext> supplier;

   LuaContextPool(Supplier<LuaContext> supplier, int min, int max, long validationInterval) {
      this.supplier = supplier;
      pool = new ConcurrentLinkedQueue<>();
      for (int i = 0; i < min; i++) {
         pool.add(this.supplier.get());
      }
      executorService = Executors.newSingleThreadScheduledExecutor();
      executorService.scheduleWithFixedDelay(() -> {
         int size = pool.size();
         if (size < min) {
            int sizeToBeAdded = min + size;
            for (int i = 0; i < sizeToBeAdded; i++) {
               pool.add(this.supplier.get());
            }
         } else if (size > max) {
            int sizeToBeRemoved = size - max;
            for (int i = 0; i < sizeToBeRemoved; i++) {
               Util.close(pool.poll());
            }
         }
      }, validationInterval, validationInterval, TimeUnit.SECONDS);
   }

   public LuaContext borrow() {
      LuaContext engine;
      if ((engine = pool.poll()) == null) {
         engine = supplier.get();
      }
      return engine;
   }

   public void returnToPool(LuaContext lua) {
      if (lua == null) {
         return;
      }
      this.pool.offer(lua);
   }

   public void shutdown() {
      if (executorService != null) {
         executorService.shutdown();
      }
      pool.forEach(LuaContext::close);
      pool.clear();
   }
}
