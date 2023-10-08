package org.infinispan.query.clustered;

import java.util.BitSet;
import java.util.UUID;
import java.util.concurrent.CompletionStage;

import org.infinispan.Cache;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.query.clustered.commandworkers.CQCommandType;
import org.infinispan.query.impl.QueryDefinition;

/**
 * @since 10.1
 */
@ProtoTypeId(ProtoStreamTypeIds.CLUSTERED_QUERY_OPERATION)
public final class ClusteredQueryOperation {

   @ProtoField(1)
   final CQCommandType commandType;

   @ProtoField(2)
   final QueryDefinition queryDefinition;

   // identifies the query
   @ProtoField(3)
   final UUID queryId;

   // for retrieve keys on a lazy query
   @ProtoField(value = 4, defaultValue = "0")
   final int docIndex;

   @ProtoFactory
   public ClusteredQueryOperation(CQCommandType commandType, QueryDefinition queryDefinition, UUID queryId, int docIndex) {
      this.commandType = commandType;
      this.queryDefinition = queryDefinition;
      this.queryId = queryId;
      this.docIndex = docIndex;
   }

   private ClusteredQueryOperation(CQCommandType commandType, QueryDefinition queryDefinition) {
      this(commandType, queryDefinition, null, 0);
   }

   public QueryDefinition getQueryDefinition() {
      return queryDefinition;
   }

   static ClusteredQueryOperation getResultSize(QueryDefinition queryDefinition) {
      return new ClusteredQueryOperation(CQCommandType.GET_RESULT_SIZE, queryDefinition);
   }

   static ClusteredQueryOperation delete(QueryDefinition queryDefinition) {
      return new ClusteredQueryOperation(CQCommandType.DELETE, queryDefinition);
   }

   static ClusteredQueryOperation createEagerIterator(QueryDefinition queryDefinition) {
      return new ClusteredQueryOperation(CQCommandType.CREATE_EAGER_ITERATOR, queryDefinition);
   }

   public CompletionStage<QueryResponse> perform(Cache<?, ?> cache, BitSet segments) {
      return commandType.perform(cache.getAdvancedCache(), queryDefinition, queryId, docIndex, segments);
   }
}
