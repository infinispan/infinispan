package org.infinispan.query.remote.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.infinispan.commons.marshall.SerializeWith;
import org.infinispan.protostream.MessageMarshaller;
import org.infinispan.protostream.WrappedMessage;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
@SerializeWith(Externalizers.QueryRequestExternalizer.class)
public final class QueryRequest {

   private String queryString;

   private List<NamedParameter> namedParameters;

   private Long startOffset;

   private Integer maxResults;

   private String indexedQueryMode;

   public String getQueryString() {
      return queryString;
   }

   public void setQueryString(String queryString) {
      this.queryString = queryString;
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

   public Map<String, Object> getNamedParametersMap() {
      if (namedParameters == null || namedParameters.isEmpty()) {
         return null;
      }
      Map<String, Object> params = new HashMap<>(namedParameters.size());
      for (NamedParameter p : namedParameters) {
         params.put(p.getName(), p.getValue());
      }
      return params;
   }

   public void setIndexedQueryMode(String indexedQueryMode) {
      this.indexedQueryMode = indexedQueryMode;
   }

   public String getIndexedQueryMode() {
      return indexedQueryMode;
   }

   static final class Marshaller implements MessageMarshaller<QueryRequest> {

      @Override
      public QueryRequest readFrom(ProtoStreamReader reader) throws IOException {
         QueryRequest queryRequest = new QueryRequest();
         queryRequest.setQueryString(reader.readString("queryString"));
         queryRequest.setStartOffset(reader.readLong("startOffset"));
         queryRequest.setMaxResults(reader.readInt("maxResults"));
         queryRequest.setNamedParameters(reader.readCollection("namedParameters", new ArrayList<>(), NamedParameter.class));
         queryRequest.setIndexedQueryMode(reader.readString("indexedQueryMode"));
         return queryRequest;
      }

      @Override
      public void writeTo(ProtoStreamWriter writer, QueryRequest queryRequest) throws IOException {
         writer.writeString("queryString", queryRequest.getQueryString());
         writer.writeLong("startOffset", queryRequest.getStartOffset());
         writer.writeInt("maxResults", queryRequest.getMaxResults());
         writer.writeCollection("namedParameters", queryRequest.getNamedParameters(), NamedParameter.class);
         writer.writeString("indexedQueryMode", queryRequest.getIndexedQueryMode());
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

   @SerializeWith(Externalizers.NamedParameterExternalizer.class)
   public static final class NamedParameter {

      private final String name;

      private final Object value;

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
         public Class<NamedParameter> getJavaClass() {
            return NamedParameter.class;
         }

         @Override
         public String getTypeName() {
            return "org.infinispan.query.remote.client.QueryRequest.NamedParameter";
         }
      }
   }
}
