package org.infinispan.query.clustered;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.query.impl.externalizers.ExternalizerIds;

/**
 * The response for a {@link ClusteredQueryOperation}.
 *
 * @author Israel Lacerra &lt;israeldl@gmail.com&gt;
 * @since 5.1
 */
public final class QueryResponse {

   private final NodeTopDocs nodeTopDocs;
   private final long resultSize;

   public QueryResponse(long resultSize) {
      this.resultSize = resultSize;
      nodeTopDocs = null;
   }

   public QueryResponse(NodeTopDocs nodeTopDocs) {
      this.resultSize = nodeTopDocs.totalHitCount;
      this.nodeTopDocs = nodeTopDocs;
   }

   public NodeTopDocs getNodeTopDocs() {
      return nodeTopDocs;
   }

   public Long getResultSize() {
      return resultSize;
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
            output.writeLong(queryResponse.resultSize);
         }
      }

      @Override
      public QueryResponse readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         NodeTopDocs nodeTopDocs = (NodeTopDocs) input.readObject();
         if (nodeTopDocs != null) {
            return new QueryResponse(nodeTopDocs);
         }
         return new QueryResponse(input.readLong());
      }
   }
}
