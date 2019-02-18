package org.infinispan.commands;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * CancellationServiceImpl is a default implementation of {@link CancellationService}
 *
 * @author Vladimir Blagojevic
 * @since 5.2
 */
public class CancellationServiceImpl implements CancellationService {

   private static final Log log = LogFactory.getLog(CancellationServiceImpl.class);
   private final Map<UUID, Thread> commandThreadMap = new ConcurrentHashMap<>();

   @Override
   public void register(Thread t, UUID id) {
      commandThreadMap.put(id, t);
   }

   @Override
   public void unregister(UUID id) {
      commandThreadMap.remove(id);
   }

   @Override
   public void cancel(UUID id) {
      Thread thread = commandThreadMap.get(id);
      if (thread != null) {
         log.trace("Calling interrupt on thread " + thread);
         thread.interrupt();
      } else{
         log.couldNotInterruptThread(id);
      }
   }
}
