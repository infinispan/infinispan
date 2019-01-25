package org.infinispan.rest.framework.impl;

import java.security.Principal;
import java.util.List;
import java.util.Map;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.rest.framework.ContentSource;
import org.infinispan.rest.framework.Method;
import org.infinispan.rest.framework.RestRequest;

import io.netty.handler.codec.http.QueryStringDecoder;

/**
 * @since 10.0
 */
public class SimpleRequest implements RestRequest {
   private final Method method;
   private final String path;
   private Map<String, String> headers;
   private final ContentSource contents;
   private Map<String, String> variables;
   private String action;
   private Principal principal;

   private SimpleRequest(Method method, String path, Map<String, String> headers, ContentSource contents) {
      QueryStringDecoder queryStringDecoder = new QueryStringDecoder(path);
      Map<String, List<String>> parameters = queryStringDecoder.parameters();
      this.path = queryStringDecoder.path();
      this.headers = headers;
      List<String> action = parameters.get("action");
      if (action != null) {
         this.action = action.iterator().next();
      }
      this.method = method;
      this.contents = contents;
   }

   @Override
   public Method method() {
      return method;
   }

   @Override
   public String path() {
      return path;
   }

   @Override
   public ContentSource contents() {
      return contents;
   }

   @Override
   public Map<String, List<String>> parameters() {
      return null;
   }

   @Override
   public Map<String, String> variables() {
      return variables;
   }

   @Override
   public String getAction() {
      return action;
   }

   @Override
   public MediaType contentType() {
      return MediaType.MATCH_ALL;
   }

   @Override
   public MediaType keyContentType() {
      return MediaType.MATCH_ALL;
   }

   @Override
   public String getAcceptHeader() {
      return null;
   }

   @Override
   public String getAuthorizationHeader() {
      return null;
   }

   @Override
   public String getCacheControlHeader() {
      return null;
   }

   @Override
   public String getContentTypeHeader() {
      return null;
   }


   @Override
   public String getEtagIfMatchHeader() {
      return null;
   }

   @Override
   public String getEtagIfModifiedSinceHeader() {
      return null;
   }

   @Override
   public String getEtagIfNoneMatchHeader() {
      return null;
   }

   @Override
   public String getEtagIfUnmodifiedSinceHeader() {
      return null;
   }

   @Override
   public Long getMaxIdleTimeSecondsHeader() {
      return null;
   }

   @Override
   public boolean getPerformAsyncHeader() {
      return false;
   }

   @Override
   public Long getTimeToLiveSecondsHeader() {
      return null;
   }

   @Override
   public Long getCreatedHeader() {
      return null;
   }

   @Override
   public Long getLastUsedHeader() {
      return null;
   }

   @Override
   public Principal getPrincipal() {
      return principal;
   }

   @Override
   public void setPrincipal(Principal principal) {
      this.principal = principal;
   }

   @Override
   public void setVariables(Map<String, String> variables) {
      this.variables = variables;
   }

   @Override
   public void setAction(String action) {
      this.action = action;
   }

   public static class Builder implements RestRequestBuilder<Builder> {
      private Method method;
      private String path;
      private Map<String, String> headers;
      private ContentSource contents;

      public Builder setMethod(Method method) {
         this.method = method;
         return this;
      }

      public Builder setPath(String path) {
         this.path = path;
         return this;
      }

      public Builder setHeaders(Map<String, String> headers) {
         this.headers = headers;
         return this;
      }

      public Builder setContents(ContentSource contents) {
         this.contents = contents;
         return this;
      }

      public SimpleRequest build() {
         return new SimpleRequest(method, path, headers, contents);
      }
   }
}
