package org.infinispan.executors;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;

/**
 * An abstract class that encapsulates the logic of sharing an {@link ExecutorService} or a {@link ScheduledExecutorService}.
 *
 * @author Manik Surtani
 * @version 4.2
 *
 * @see DefaultSharedExecutorFactory
 * @see DefaultSharedScheduledExecutorFactory
 */
public abstract class AbstractSharedExecutorFactory<E extends ExecutorService> {

   private final Map<Properties, E> executors = new HashMap<Properties, E>(2, 0.9f);

   protected abstract E createService(Properties p);

   protected E getOrCreateService(Properties p) {
      synchronized (executors) {
         E e = executors.get(p);
         if (e == null) {
            e = createService(p);
            executors.put(p, e);
         }
         return e;
      }
   }
}
