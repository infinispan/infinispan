package org.infinispan.query.remote.client.impl;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;

import org.infinispan.protostream.WrappedMessage;
import org.jboss.marshalling.Externalizer;

/**
 * jboss-marshalling externalizers for QueryRequest and QueryResponse objects.
 *
 * @author anistor@redhat.com
 * @since 9.1
 */
public final class Externalizers {

   private Externalizers() {
   }

   public static final class QueryRequestExternalizer implements Externalizer {

      @Override
      public void writeExternal(Object object, ObjectOutput output) throws IOException {
         QueryRequest queryRequest = (QueryRequest) object;
         output.writeUTF(queryRequest.getQueryString());
         output.writeLong(queryRequest.getStartOffset() != null ? queryRequest.getStartOffset() : -1);
         output.writeInt(queryRequest.getMaxResults() != null ? queryRequest.getMaxResults() : -1);
         output.writeObject(queryRequest.getNamedParameters());
         output.writeBoolean(queryRequest.isLocal());
         output.writeInt(queryRequest.hitCountAccuracy() != null ? queryRequest.hitCountAccuracy() : -1);
      }

      @Override
      public Object createExternal(Class<?> aClass, ObjectInput input) throws IOException, ClassNotFoundException {
         QueryRequest queryRequest = new QueryRequest();
         queryRequest.setQueryString(input.readUTF());
         long startOffset = input.readLong();
         queryRequest.setStartOffset(startOffset != -1 ? startOffset : null);
         int maxResults = input.readInt();
         queryRequest.setMaxResults(maxResults != -1 ? maxResults : null);
         queryRequest.setNamedParameters((List<QueryRequest.NamedParameter>) input.readObject());
         queryRequest.setLocal(input.readBoolean());
         int hitCountAccuracy = input.readInt();
         queryRequest.hitCountAccuracy(hitCountAccuracy != -1 ? hitCountAccuracy : null);
         return queryRequest;
      }
   }

   public static final class NamedParameterExternalizer implements Externalizer {

      @Override
      public void writeExternal(Object object, ObjectOutput output) throws IOException {
         QueryRequest.NamedParameter namedParameter = (QueryRequest.NamedParameter) object;
         output.writeUTF(namedParameter.getName());
         output.writeObject(namedParameter.getValue());
      }

      @Override
      public Object createExternal(Class<?> aClass, ObjectInput input) throws IOException, ClassNotFoundException {
         String name = input.readUTF();
         Object value = input.readObject();
         return new QueryRequest.NamedParameter(name, value);
      }
   }

   public static final class QueryResponseExternalizer implements Externalizer {

      @Override
      public void writeExternal(Object object, ObjectOutput output) throws IOException {
         QueryResponse queryResponse = (QueryResponse) object;
         output.writeInt(queryResponse.getNumResults());
         output.writeInt(queryResponse.getProjectionSize());
         List<WrappedMessage> wrappedResults = queryResponse.getResults();
         List<Object> results = new ArrayList<>(wrappedResults.size());
         for (WrappedMessage o : wrappedResults) {
            results.add(o.getValue());
         }
         output.writeObject(results);
         output.writeInt(queryResponse.hitCount());
         output.writeBoolean(queryResponse.hitCountExact());
      }

      @Override
      public Object createExternal(Class<?> aClass, ObjectInput input) throws IOException, ClassNotFoundException {
         QueryResponse queryResponse = new QueryResponse();
         queryResponse.setNumResults(input.readInt());
         queryResponse.setProjectionSize(input.readInt());
         List<Object> results = (List<Object>) input.readObject();
         List<WrappedMessage> wrappedResults = new ArrayList<>(results.size());
         for (Object o : results) {
            wrappedResults.add(new WrappedMessage(o));
         }
         queryResponse.setResults(wrappedResults);
         queryResponse.hitCount(input.readInt());
         queryResponse.hitCountExact(input.readBoolean());
         return queryResponse;
      }
   }
}
