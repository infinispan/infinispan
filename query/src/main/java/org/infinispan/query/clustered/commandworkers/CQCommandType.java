package org.infinispan.query.clustered.commandworkers;

import java.util.BitSet;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.Proto;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.query.clustered.QueryResponse;
import org.infinispan.query.impl.QueryDefinition;

/**
 * Types of CQWorker. Each type defines a different behavior for a ClusteredQueryCommand. The {@link #perform} method is
 * delegated to a CQWorker. This enum is more efficient to serialize than an actual CQWorker.
 *
 * @author Israel Lacerra &lt;israeldl@gmail.com&gt;
 * @author anistor@redhat.com
 * @since 5.1
 */
@Proto
@ProtoTypeId(ProtoStreamTypeIds.CQ_COMMAND_TYPE)
public enum CQCommandType {

   //TODO [ISPN-12182] Add support for scrolling
   CREATE_EAGER_ITERATOR(CQCreateEagerQuery::new),
   GET_RESULT_SIZE(CQGetResultSize::new),
   DELETE(CQDelete::new);

   private static final CQCommandType[] CACHED_VALUES = values();

   public static CQCommandType valueOf(int ordinal) {
      return CACHED_VALUES[ordinal];
   }

   private final Supplier<CQWorker> workerSupplier;

   CQCommandType(Supplier<CQWorker> workerSupplier) {
      this.workerSupplier = workerSupplier;
   }

   public CompletionStage<QueryResponse> perform(AdvancedCache<?, ?> cache, QueryDefinition queryDefinition,
                                                 UUID queryId, int docIndex, BitSet segments) {
      CQWorker worker = workerSupplier.get();
      worker.initialize(cache.withStorageMediaType(), queryDefinition, queryId, docIndex);
      return worker.perform(segments);
   }
}
