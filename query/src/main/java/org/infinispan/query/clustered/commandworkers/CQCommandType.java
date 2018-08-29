package org.infinispan.query.clustered.commandworkers;

import java.util.UUID;
import java.util.function.Supplier;

import org.infinispan.AdvancedCache;
import org.infinispan.query.clustered.QueryResponse;
import org.infinispan.query.impl.QueryDefinition;

/**
 * Types of CQWorker. Each type defines a different behavior for a ClusteredQueryCommand. The {@link #perform} method is
 * delegated to a CQWorker. This enum is more efficient to serialize than an actual CQWorker.
 *
 * @author Israel Lacerra <israeldl@gmail.com>
 * @author anistor@redhat.com
 * @since 5.1
 */
public enum CQCommandType {

   CREATE_LAZY_ITERATOR(CQCreateLazyQuery::new),
   CREATE_EAGER_ITERATOR(CQCreateEagerQuery::new),
   DESTROY_LAZY_ITERATOR(CQKillLazyIterator::new),
   GET_SOME_KEYS(CQLazyFetcher::new),
   GET_RESULT_SIZE(CQGetResultSize::new);

   private static final CQCommandType[] CACHED_VALUES = values();

   public static CQCommandType valueOf(int ordinal) {
      return CACHED_VALUES[ordinal];
   }

   private final Supplier<CQWorker> workerSupplier;

   CQCommandType(Supplier<CQWorker> workerSupplier) {
      this.workerSupplier = workerSupplier;
   }

   public QueryResponse perform(AdvancedCache<?, ?> cache, QueryDefinition queryDefinition, UUID queryId, int docIndex) {
      CQWorker worker = workerSupplier.get();
      worker.init(cache, queryDefinition, queryId, docIndex);
      return worker.perform();
   }
}
