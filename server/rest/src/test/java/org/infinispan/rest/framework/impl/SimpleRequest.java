package org.infinispan.rest.framework.impl;

import java.net.InetSocketAddress;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

import javax.security.auth.Subject;

import org.infinispan.commons.api.CacheContainerAdmin.AdminFlag;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.context.Flag;
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
   private Subject subject;

   private SimpleRequest(Method method, String path, Map<String, String> headers, ContentSource contents, Subject subject) {
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
      this.subject = subject;
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
   public String uri() {
      return null;
   }

   @Override
   public String header(String name) {
      return null;
   }

   @Override
   public List<String> headers(String name) {
      return null;
   }

   @Override
   public InetSocketAddress getRemoteAddress() {
      return null;
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
   public String getParameter(String name) {
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
   public String getCookieHeader() {
      return null;
   }


   @Override
   public String getEtagIfMatchHeader() {
      return null;
   }

   @Override
   public String getIfModifiedSinceHeader() {
      return null;
   }

   @Override
   public String getEtagIfNoneMatchHeader() {
      return null;
   }

   @Override
   public String getIfUnmodifiedSinceHeader() {
      return null;
   }

   @Override
   public Long getMaxIdleTimeSecondsHeader() {
      return null;
   }

   @Override
   public Long getTimeToLiveSecondsHeader() {
      return null;
   }

   @Override
   public EnumSet<AdminFlag> getAdminFlags() {
      return null;
   }

   @Override
   public Flag[] getFlags() {
      return new Flag[0];
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
   public Subject getSubject() {
      return subject;
   }

   @Override
   public void setSubject(Subject subject) {
      this.subject = subject;
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
      private Subject subject;

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

      public Builder setSubject(Subject subject) {
         this.subject = subject;
         return this;
      }

      public SimpleRequest build() {
         return new SimpleRequest(method, path, headers, contents, subject);
      }
   }
}
