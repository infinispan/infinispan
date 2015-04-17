package org.infinispan.hibernate.search.impl;

import org.hibernate.search.engine.service.spi.Startable;
import org.hibernate.search.engine.service.spi.Stoppable;
import org.hibernate.search.spi.BuildContext;
import org.hibernate.search.util.impl.Executors;
import org.hibernate.search.util.logging.impl.LoggerFactory;
import org.infinispan.hibernate.search.logging.Log;
import org.kohsuke.MetaInfServices;

import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * A shared service used among all InfinispanDirectoryProvider instances to delete segments asynchronously.
 *
 * @author Sanne Grinovero <sanne@hibernate.org> (C) 2014 Red Hat Inc.
 */
@MetaInfServices(AsyncDeleteExecutorService.class)
public class DefaultAsyncDeleteExecutor implements AsyncDeleteExecutorService, Startable, Stoppable {

   private static final Log log = LoggerFactory.make(Log.class);

   private ThreadPoolExecutor threadPool;

   @Override
   public void start(Properties properties, BuildContext context) {
      threadPool = Executors
            .newScalableThreadPool(1, 5, "async deletion of index segments", 100);
   }

   @Override
   public void stop() {
      closeAndFlush();
   }

   @Override
   public Executor getExecutor() {
      return threadPool;
   }

   @Override
   public void closeAndFlush() {
      //Each DirectoryProvider using this service should flush and wait a bit to allow
      //async work to be performed before the Directory itself becomes unavailable.
      threadPool.shutdown();
      try {
         threadPool.awaitTermination(30L, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
         log.interruptedWhileWaitingForAsyncDeleteFlush();
      }
   }

}
