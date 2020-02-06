package org.infinispan.query.clustered;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.BitSet;
import java.util.Collections;
import java.util.Set;
import java.util.UUID;

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

   private CQCommandType commandType;

   private QueryDefinition queryDefinition;

   // identifies the query
   private UUID queryId;

   // for retrieve keys on a lazy query
   private int docIndex = 0;

   private ClusteredQueryOperation(CQCommandType commandType) {
      this.commandType = commandType;
   }

   public QueryDefinition getQueryDefinition() {
      return queryDefinition;
   }

   static ClusteredQueryOperation createLazyIterator(QueryDefinition queryDefinition, UUID queryId) {
      ClusteredQueryOperation cmd = new ClusteredQueryOperation(CQCommandType.CREATE_LAZY_ITERATOR);
      cmd.queryDefinition = queryDefinition;
      cmd.queryId = queryId;
      return cmd;
   }

   static ClusteredQueryOperation getResultSize(QueryDefinition queryDefinition) {
      ClusteredQueryOperation cmd = new ClusteredQueryOperation(CQCommandType.GET_RESULT_SIZE);
      cmd.queryDefinition = queryDefinition;
      return cmd;
   }

   static ClusteredQueryOperation createEagerIterator(QueryDefinition queryDefinition) {
      ClusteredQueryOperation cmd = new ClusteredQueryOperation(CQCommandType.CREATE_EAGER_ITERATOR);
      cmd.queryDefinition = queryDefinition;
      return cmd;
   }

   static ClusteredQueryOperation destroyLazyQuery(UUID queryId) {
      ClusteredQueryOperation cmd = new ClusteredQueryOperation(CQCommandType.DESTROY_LAZY_ITERATOR);
      cmd.queryId = queryId;
      return cmd;
   }

   static ClusteredQueryOperation retrieveKeyFromLazyQuery(UUID queryId, int docIndex) {
      ClusteredQueryOperation cmd = new ClusteredQueryOperation(CQCommandType.GET_SOME_KEYS);
      cmd.queryId = queryId;
      cmd.docIndex = docIndex;
      return cmd;
   }

   public QueryResponse perform(Cache<?, ?> cache, BitSet segments) {
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
         ClusteredQueryOperation clusteredQueryOperation = new ClusteredQueryOperation(commandType);
         clusteredQueryOperation.queryDefinition = queryDefinition;
         clusteredQueryOperation.queryId = queryId;
         clusteredQueryOperation.docIndex = docIndex;
         return clusteredQueryOperation;
      }
   }
}
