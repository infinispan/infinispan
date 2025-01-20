package org.infinispan.server.resp.scripting;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * A pool for {@link LuaContext} objects.
 */
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
               pool.poll().shutdown();
            }
         }
      }, validationInterval, validationInterval, TimeUnit.SECONDS);
   }

   LuaContext borrow() {
      LuaContext engine;
      if ((engine = pool.poll()) == null) {
         engine = supplier.get();
      }
      engine.pool = this;
      return engine;
   }

   void returnToPool(LuaContext lua) {
      if (lua == null) {
         return;
      }
      if (lua.lua.getTop() > 0) {
         throw new RuntimeException("Lua stack was not empty: " + lua.lua.getTop());
      }
      lua.pool = null;
      this.pool.offer(lua);
   }

   /**
    * Clears the pool
    */
   public void invalidate() {
      List<LuaContext> contexts = new ArrayList<>(pool);
      pool.clear();
      contexts.forEach(LuaContext::shutdown);
   }

   /**
    * Shuts down this pool
    */
   public void shutdown() {
      if (executorService != null) {
         executorService.shutdown();
      }
      invalidate();
   }
}
