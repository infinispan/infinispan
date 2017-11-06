package org.infinispan.rest.search;

import static org.infinispan.rest.JSONConstants.CAUSE;
import static org.infinispan.rest.JSONConstants.ERROR;
import static org.infinispan.rest.JSONConstants.MESSAGE;

import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.annotate.JsonPropertyOrder;
import org.codehaus.jackson.annotate.JsonTypeInfo;
import org.codehaus.jackson.annotate.JsonTypeName;

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
