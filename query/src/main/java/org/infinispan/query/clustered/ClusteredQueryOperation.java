package org.infinispan.query.clustered;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.BitSet;
import java.util.Collections;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletionStage;

import org.infinispan.Cache;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.query.clustered.commandworkers.CQCommandType;
import org.infinispan.query.impl.QueryDefinition;
import org.infinispan.query.impl.externalizers.ExternalizerIds;

/**
 * @since 10.1
 */
public final class ClusteredQueryOperation {

   private final CQCommandType commandType;

   private final QueryDefinition queryDefinition;

   // identifies the query
   private UUID queryId;

   // for retrieve keys on a lazy query
   private int docIndex = 0;

   private ClusteredQueryOperation(CQCommandType commandType, QueryDefinition queryDefinition) {
      this.commandType = commandType;
      this.queryDefinition = queryDefinition;
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

   public static final class Externalizer implements AdvancedExternalizer<ClusteredQueryOperation> {

      @Override
      public Set<Class<? extends ClusteredQueryOperation>> getTypeClasses() {
         return Collections.singleton(ClusteredQueryOperation.class);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.CLUSTERED_QUERY_OPERATION;
      }

      @Override
      public void writeObject(ObjectOutput output, ClusteredQueryOperation object) throws IOException {
         MarshallUtil.marshallEnum(object.commandType, output);
         output.writeObject(object.queryDefinition);
         MarshallUtil.marshallUUID(object.queryId, output, true);
         output.writeInt(object.docIndex);
      }

      @Override
      public ClusteredQueryOperation readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         CQCommandType commandType = MarshallUtil.unmarshallEnum(input, CQCommandType::valueOf);
         QueryDefinition queryDefinition = (QueryDefinition) input.readObject();
         UUID queryId = MarshallUtil.unmarshallUUID(input, true);
         int docIndex = input.readInt();
         ClusteredQueryOperation clusteredQueryOperation = new ClusteredQueryOperation(commandType, queryDefinition);
         clusteredQueryOperation.queryId = queryId;
         clusteredQueryOperation.docIndex = docIndex;
         return clusteredQueryOperation;
      }
   }
}
