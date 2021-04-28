package org.infinispan.query.remote.client.impl;

import static java.util.stream.Collectors.toList;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.dataconversion.internal.JsonSerialization;
import org.infinispan.commons.marshall.SerializeWith;
import org.infinispan.protostream.MessageMarshaller;
import org.infinispan.protostream.WrappedMessage;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
@SerializeWith(Externalizers.QueryRequestExternalizer.class)
public final class QueryRequest implements JsonSerialization {

   public static final String QUERY_STRING_FIELD = "queryString";
   public static final String START_OFFSET_FIELD = "startOffset";
   public static final String MAX_RESULTS_FIELD = "maxResults";
   public static final String NAMED_PARAMETERS_FIELD = "namedParameters";
   private String queryString;

   private List<NamedParameter> namedParameters;

   private Long startOffset;

   private Integer maxResults;

   public String getQueryString() {
      return queryString;
   }

   public void setQueryString(String queryString) {
      this.queryString = queryString;
   }

   public Long getStartOffset() {
      return startOffset == null ? Long.valueOf(-1) : startOffset;
   }

   public void setStartOffset(Long startOffset) {
      this.startOffset = startOffset;
   }

   public Integer getMaxResults() {
      return maxResults == null ? Integer.valueOf(-1) : maxResults;
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

   public static QueryRequest fromJson(Json jsonRequest) {
      String queryString = jsonRequest.at(QUERY_STRING_FIELD).asString();
      Json offsetValue = jsonRequest.at(START_OFFSET_FIELD);
      Json maxResults = jsonRequest.at(MAX_RESULTS_FIELD);
      List<NamedParameter> params = jsonRequest.at(NAMED_PARAMETERS_FIELD).asJsonList().stream()
            .map(NamedParameter::fromJson).collect(toList());

      QueryRequest queryRequest = new QueryRequest();
      queryRequest.setQueryString(queryString);
      if (!offsetValue.isNull()) queryRequest.setStartOffset(offsetValue.asLong());
      if (!maxResults.isNull()) queryRequest.setMaxResults(maxResults.asInteger());
      if (!params.isEmpty()) queryRequest.setNamedParameters(params);

      return queryRequest;
   }

   @Override
   public Json toJson() {
      return Json.object()
            .set(QUERY_STRING_FIELD, queryString)
            .set(START_OFFSET_FIELD, startOffset)
            .set(MAX_RESULTS_FIELD, maxResults)
            .set(NAMED_PARAMETERS_FIELD, Json.make(getNamedParameters()));
   }

   static final class Marshaller implements MessageMarshaller<QueryRequest> {

      @Override
      public QueryRequest readFrom(ProtoStreamReader reader) throws IOException {
         QueryRequest queryRequest = new QueryRequest();
         queryRequest.setQueryString(reader.readString("queryString"));
         queryRequest.setStartOffset(reader.readLong("startOffset"));
         queryRequest.setMaxResults(reader.readInt("maxResults"));
         queryRequest.setNamedParameters(reader.readCollection("namedParameters", new ArrayList<>(), NamedParameter.class));
         return queryRequest;
      }

      @Override
      public void writeTo(ProtoStreamWriter writer, QueryRequest queryRequest) throws IOException {
         writer.writeString("queryString", queryRequest.getQueryString());
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

   @SerializeWith(Externalizers.NamedParameterExternalizer.class)
   public static final class NamedParameter implements JsonSerialization {
      public static final String NAME_FIELD = "name";
      public static final String VALUE_FIELD = "value";

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

      @Override
      public Json toJson() {
         return Json.object(NAME_FIELD, name).set(VALUE_FIELD, value);
      }

      public static NamedParameter fromJson(Json source) {
         String name = source.at(NAME_FIELD).asString();
         Object value = source.at(VALUE_FIELD).getValue();
         return new NamedParameter(name, value);
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
