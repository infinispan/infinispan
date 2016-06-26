package org.infinispan.server.core.test;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.core.AbstractProtocolServer;
import org.infinispan.test.TestingUtil;

import java.util.function.Consumer;

/**
 * Stoppable implements simple wrappers for objects which need to be stopped in certain way after being used
 * @author Galder Zamarreño
 * @author wburns
 */
public class Stoppable {
   public static <T extends EmbeddedCacheManager> void useCacheManager(T stoppable, Consumer<? super T> consumer) {
      try {
         consumer.accept(stoppable);
      } finally {
         TestingUtil.killCacheManagers(stoppable);
      }
   }

   public static <T extends AbstractProtocolServer<?>> void useServer(T stoppable, Consumer<? super T> consumer) {
      try {
         consumer.accept(stoppable);
      } finally {
         ServerTestingUtil.killServer(stoppable);
      }
   }

}
