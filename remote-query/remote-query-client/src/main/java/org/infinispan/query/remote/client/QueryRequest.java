package org.infinispan.query.remote.client;

import org.infinispan.protostream.MessageMarshaller;

import java.io.IOException;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
public final class QueryRequest {

   private String jpqlString;

   private long startOffset;

   private int maxResults;

   public String getJpqlString() {
      return jpqlString;
   }

   public void setJpqlString(String jpqlString) {
      this.jpqlString = jpqlString;
   }

   public long getStartOffset() {
      return startOffset;
   }

   public void setStartOffset(long startOffset) {
      this.startOffset = startOffset;
   }

   public int getMaxResults() {
      return maxResults;
   }

   public void setMaxResults(int maxResults) {
      this.maxResults = maxResults;
   }

   public static final class Marshaller implements MessageMarshaller<QueryRequest> {

      @Override
      public QueryRequest readFrom(ProtoStreamReader reader) throws IOException {
         QueryRequest queryRequest = new QueryRequest();
         queryRequest.setJpqlString(reader.readString("jpqlString"));
         queryRequest.setStartOffset(reader.readLong("startOffset"));
         queryRequest.setMaxResults(reader.readInt("maxResults"));
         return queryRequest;
      }

      @Override
      public void writeTo(ProtoStreamWriter writer, QueryRequest remoteQuery) throws IOException {
         writer.writeString("jpqlString", remoteQuery.getJpqlString());
         writer.writeLong("startOffset", remoteQuery.getStartOffset());
         writer.writeInt("maxResults", remoteQuery.getMaxResults());
      }

      @Override
      public Class<QueryRequest> getJavaClass() {
         return QueryRequest.class;
      }

      @Override
      public String getTypeName() {
         return "org.infinispan.query.remote.client.QueryRequest";
      }
   }
}
