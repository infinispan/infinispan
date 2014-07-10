package org.infinispan.query.remote.client;

import org.infinispan.protostream.MessageMarshaller;

import java.io.IOException;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
public class QueryRequestMarshaller implements MessageMarshaller<QueryRequest> {

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
