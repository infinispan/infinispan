package org.infinispan.query.remote.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.infinispan.protostream.MessageMarshaller;
import org.infinispan.protostream.WrappedMessage;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
public final class QueryRequest {

   private String jpqlString;

   private List<NamedParameter> namedParameters;

   private Long startOffset;

   private Integer maxResults;

   public String getJpqlString() {
      return jpqlString;
   }

   public void setJpqlString(String jpqlString) {
      this.jpqlString = jpqlString;
   }

   public Long getStartOffset() {
      return startOffset;
   }

   public void setStartOffset(Long startOffset) {
      this.startOffset = startOffset;
   }

   public Integer getMaxResults() {
      return maxResults;
   }

   public void setMaxResults(Integer maxResults) {
      this.maxResults = maxResults;
   }

   public List<NamedParameter> getNamedParameters() {
      return namedParameters;
   }

   public void setNamedParameters(List<NamedParameter> namedParameters) {
      this.namedParameters = namedParameters;
   }

   static final class Marshaller implements MessageMarshaller<QueryRequest> {

      @Override
      public QueryRequest readFrom(ProtoStreamReader reader) throws IOException {
         QueryRequest queryRequest = new QueryRequest();
         queryRequest.setJpqlString(reader.readString("jpqlString"));
         queryRequest.setStartOffset(reader.readLong("startOffset"));
         queryRequest.setMaxResults(reader.readInt("maxResults"));
         queryRequest.setNamedParameters(reader.readCollection("namedParameters", new ArrayList<>(), NamedParameter.class));
         return queryRequest;
      }

      @Override
      public void writeTo(ProtoStreamWriter writer, QueryRequest queryRequest) throws IOException {
         writer.writeString("jpqlString", queryRequest.getJpqlString());
         writer.writeLong("startOffset", queryRequest.getStartOffset());
         writer.writeInt("maxResults", queryRequest.getMaxResults());
         writer.writeCollection("namedParameters", queryRequest.getNamedParameters(), NamedParameter.class);
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

   public static final class NamedParameter {

      private String name;

      private Object value;

      public NamedParameter(String name, Object value) {
         if (name == null) {
            throw new IllegalArgumentException("name cannot be null");
         }
         if (value == null) {
            throw new IllegalArgumentException("value cannot be null");
         }
         this.name = name;
         this.value = value;
      }

      public String getName() {
         return name;
      }

      public Object getValue() {
         return value;
      }

      static final class Marshaller implements MessageMarshaller<NamedParameter> {

         @Override
         public NamedParameter readFrom(ProtoStreamReader reader) throws IOException {
            String name = reader.readString("name");
            WrappedMessage value = reader.readObject("value", WrappedMessage.class);
            return new NamedParameter(name, value.getValue());
         }

         @Override
         public void writeTo(ProtoStreamWriter writer, NamedParameter namedParameter) throws IOException {
            writer.writeString("name", namedParameter.getName());
            writer.writeObject("value", new WrappedMessage(namedParameter.getValue()), WrappedMessage.class);
         }

         @Override
         public Class<? extends NamedParameter> getJavaClass() {
            return NamedParameter.class;
         }

         @Override
         public String getTypeName() {
            return "org.infinispan.query.remote.client.QueryRequest.NamedParameter";
         }
      }
   }
}
