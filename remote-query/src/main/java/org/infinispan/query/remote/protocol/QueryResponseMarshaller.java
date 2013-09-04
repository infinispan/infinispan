package org.infinispan.query.remote.protocol;

import org.infinispan.protostream.MessageMarshaller;
import org.infinispan.protostream.WrappedMessage;

import java.io.IOException;
import java.util.ArrayList;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
public class QueryResponseMarshaller implements MessageMarshaller<QueryResponse> {

   @Override
   public String getFullName() {
      return "org.infinispan.client.hotrod.impl.query.QueryResponse";
   }

   @Override
   public QueryResponse readFrom(ProtoStreamReader reader) throws IOException {
      QueryResponse queryResponse = new QueryResponse();
      queryResponse.setNumResults(reader.readInt("numResults"));
      queryResponse.setProjectionSize(reader.readInt("projectionSize"));
      queryResponse.setResults(reader.readCollection("results", new ArrayList<WrappedMessage>(), WrappedMessage.class));
      return queryResponse;
   }

   @Override
   public void writeTo(ProtoStreamWriter writer, QueryResponse queryResponse) throws IOException {
      writer.writeInt("numResults", queryResponse.getNumResults());
      writer.writeInt("projectionSize", queryResponse.getProjectionSize());
      writer.writeCollection("results", queryResponse.getResults(), WrappedMessage.class);
   }
}
