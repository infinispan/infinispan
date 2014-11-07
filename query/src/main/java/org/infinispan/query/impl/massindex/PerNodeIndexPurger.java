package org.infinispan.query.impl.massindex;

import org.infinispan.Cache;
import org.infinispan.distexec.DefaultExecutorService;
import org.infinispan.distexec.DistributedExecutorService;
import org.infinispan.query.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Purges all indexes in the cluster, on a per node basis.
 *
 * @author gustavonalle
 * @since 7.0
 */
public class PerNodeIndexPurger implements IndexPurger {

   private static final Log LOG = LogFactory.getLog(PerNodeIndexPurger.class, Log.class);
   private static final int TIMEOUT_SECONDS = 10;
   private final Cache cache;

   public PerNodeIndexPurger(Cache cache) {
      this.cache = cache;
   }

   @Override
   public void purge() {
      DistributedExecutorService distributedExecutorService = new DefaultExecutorService(cache);
      List<Future<Void>> futures = distributedExecutorService.submitEverywhere(new IndexCleanCallable());
      for (Future f : futures) {
         try {
            f.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
         } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.errorPurgingIndexes(e);
         } catch (ExecutionException | TimeoutException e) {
            LOG.errorPurgingIndexes(e);
         }
      }
   }
}
