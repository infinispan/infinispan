package org.infinispan.query.remote.client;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;

import org.infinispan.commons.marshall.Externalizer;
import org.infinispan.protostream.WrappedMessage;

/**
 * JBMAR externalizers for QueryRequest/Response objects.
 *
 * @author anistor@redhat.com
 * @since 9.1
 */
public final class Externalizers {

   private Externalizers() {
   }

   public static final class QueryRequestExternalizer implements Externalizer<QueryRequest> {

      @Override
      public void writeObject(ObjectOutput output, QueryRequest queryRequest) throws IOException {
         output.writeUTF(queryRequest.getQueryString());
         output.writeLong(queryRequest.getStartOffset() != null ? queryRequest.getStartOffset() : -1);
         output.writeInt(queryRequest.getMaxResults() != null ? queryRequest.getMaxResults() : -1);
         output.writeObject(queryRequest.getNamedParameters());
      }

      @Override
      @SuppressWarnings("unchecked")
      public QueryRequest readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         QueryRequest queryRequest = new QueryRequest();
         queryRequest.setQueryString(input.readUTF());
         long startOffset = input.readLong();
         queryRequest.setStartOffset(startOffset != -1 ? startOffset : null);
         int maxResults = input.readInt();
         queryRequest.setMaxResults(maxResults != -1 ? maxResults : null);
         queryRequest.setNamedParameters((List<QueryRequest.NamedParameter>) input.readObject());
         return queryRequest;
      }
   }

   public static final class NamedParameterExternalizer implements Externalizer<QueryRequest.NamedParameter> {

      @Override
      public void writeObject(ObjectOutput output, QueryRequest.NamedParameter namedParameter) throws IOException {
         output.writeUTF(namedParameter.getName());
         output.writeObject(namedParameter.getValue());
      }

      @Override
      public QueryRequest.NamedParameter readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         String name = input.readUTF();
         Object value = input.readObject();
         return new QueryRequest.NamedParameter(name, value);
      }
   }

   public static final class QueryResponseExternalizer implements Externalizer<QueryResponse> {

      @Override
      public void writeObject(ObjectOutput output, QueryResponse queryResponse) throws IOException {
         output.writeInt(queryResponse.getNumResults());
         output.writeInt(queryResponse.getProjectionSize());
         List<WrappedMessage> wrappedResults = queryResponse.getResults();
         List<Object> results = new ArrayList<>(wrappedResults.size());
         for (WrappedMessage o : wrappedResults) {
            results.add(o.getValue());
         }
         output.writeObject(results);
         output.writeLong(queryResponse.getTotalResults());
      }

      @Override
      @SuppressWarnings("unchecked")
      public QueryResponse readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         QueryResponse queryResponse = new QueryResponse();
         queryResponse.setNumResults(input.readInt());
         queryResponse.setProjectionSize(input.readInt());
         List<Object> results = (List<Object>) input.readObject();
         List<WrappedMessage> wrappedResults = new ArrayList<>(results.size());
         for (Object o : results) {
            wrappedResults.add(new WrappedMessage(o));
         }
         queryResponse.setResults(wrappedResults);
         queryResponse.setTotalResults(input.readLong());
         return queryResponse;
      }
   }
}
