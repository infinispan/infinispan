package org.infinispan.query.remote.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.infinispan.commons.marshall.SerializeWith;
import org.infinispan.protostream.MessageMarshaller;
import org.infinispan.protostream.WrappedMessage;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
@SerializeWith(Externalizers.QueryResponseExternalizer.class)
public final class QueryResponse {

   private int numResults;

   private int projectionSize;

   private List<WrappedMessage> results;

   private long totalResults;

   public int getNumResults() {
      return numResults;
   }

   public void setNumResults(int numResults) {
      this.numResults = numResults;
   }

   public int getProjectionSize() {
      return projectionSize;
   }

   public void setProjectionSize(int projectionSize) {
      this.projectionSize = projectionSize;
   }

   public List<WrappedMessage> getResults() {
      return results;
   }

   public void setResults(List<WrappedMessage> results) {
      this.results = results;
   }

   public long getTotalResults() {
      return totalResults;
   }

   public void setTotalResults(long totalResults) {
      this.totalResults = totalResults;
   }

   static final class Marshaller implements MessageMarshaller<QueryResponse> {

      @Override
      public QueryResponse readFrom(ProtoStreamReader reader) throws IOException {
         QueryResponse queryResponse = new QueryResponse();
         queryResponse.setNumResults(reader.readInt("numResults"));
         queryResponse.setProjectionSize(reader.readInt("projectionSize"));
         queryResponse.setResults(reader.readCollection("results", new ArrayList<>(), WrappedMessage.class));
         queryResponse.setTotalResults(reader.readLong("totalResults"));
         return queryResponse;
      }

      @Override
      public void writeTo(ProtoStreamWriter writer, QueryResponse queryResponse) throws IOException {
         writer.writeInt("numResults", queryResponse.numResults);
         writer.writeInt("projectionSize", queryResponse.projectionSize);
         writer.writeCollection("results", queryResponse.results, WrappedMessage.class);
         writer.writeLong("totalResults", queryResponse.totalResults);
      }

      @Override
      public Class<QueryResponse> getJavaClass() {
         return QueryResponse.class;
      }

      @Override
      public String getTypeName() {
         return "org.infinispan.query.remote.client.QueryResponse";
      }
   }
}
