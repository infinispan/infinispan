package org.infinispan.rest.search;

import static org.infinispan.rest.JSONConstants.CAUSE;
import static org.infinispan.rest.JSONConstants.ERROR;
import static org.infinispan.rest.JSONConstants.MESSAGE;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonPropertyOrder({MESSAGE, CAUSE})
@JsonTypeInfo(include = JsonTypeInfo.As.WRAPPER_OBJECT, use = JsonTypeInfo.Id.NAME)
@JsonTypeName(ERROR)
@SuppressWarnings("unused")
public class QueryErrorResult implements QueryResponse {

   @JsonProperty(MESSAGE)
   private String message;

   @JsonProperty(CAUSE)
   private String cause;

   QueryErrorResult(String message, String cause) {
      this.message = message;
      this.cause = cause;
   }

   public QueryErrorResult() {
   }

   public String getMessage() {
      return message;
   }

   public String getCause() {
      return cause;
   }
}
