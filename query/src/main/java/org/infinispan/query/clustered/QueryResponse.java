package org.infinispan.query.clustered;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.query.impl.externalizers.ExternalizerIds;

/**
 * The response for a {@link ClusteredQueryCommand}.
 *
 * @author Israel Lacerra <israeldl@gmail.com>
 * @since 5.1
 */
public final class QueryResponse {

   private final NodeTopDocs nodeTopDocs;

   private final Integer resultSize;

   private final Object fetchedValue;

   public QueryResponse() {
      nodeTopDocs = null;
      resultSize = null;
      fetchedValue = null;
   }

   public QueryResponse(Object fetchedValue) {
      this.fetchedValue = fetchedValue;
      nodeTopDocs = null;
      resultSize = null;
   }

   public QueryResponse(int resultSize) {
      this.resultSize = resultSize;
      nodeTopDocs = null;
      fetchedValue = null;
   }

   public QueryResponse(NodeTopDocs nodeTopDocs) {
      this.nodeTopDocs = nodeTopDocs;
      this.resultSize = nodeTopDocs.topDocs.totalHits;
      fetchedValue = null;
   }

   public NodeTopDocs getNodeTopDocs() {
      return nodeTopDocs;
   }

   public Integer getResultSize() {
      return resultSize;
   }

   public Object getFetchedValue() {
      return fetchedValue;
   }

   public static final class Externalizer implements AdvancedExternalizer<QueryResponse> {

      @Override
      public Set<Class<? extends QueryResponse>> getTypeClasses() {
         return Collections.singleton(QueryResponse.class);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.CLUSTERED_QUERY_COMMAND_RESPONSE;
      }

      @Override
      public void writeObject(ObjectOutput output, QueryResponse queryResponse) throws IOException {
         output.writeObject(queryResponse.nodeTopDocs);
         if (queryResponse.nodeTopDocs == null) {
            output.writeObject(queryResponse.resultSize);
            if (queryResponse.resultSize == null) {
               output.writeObject(queryResponse.fetchedValue);
            }
         }
      }

      @Override
      public QueryResponse readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         NodeTopDocs nodeTopDocs = (NodeTopDocs) input.readObject();
         if (nodeTopDocs != null) {
            return new QueryResponse(nodeTopDocs);
         }
         Integer resultSize = (Integer) input.readObject();
         if (resultSize != null) {
            return new QueryResponse(resultSize.intValue());
         }
         return new QueryResponse(input.readObject());
      }
   }
}
