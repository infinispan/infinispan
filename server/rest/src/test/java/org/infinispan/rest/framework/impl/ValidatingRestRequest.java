package org.infinispan.rest.framework.impl;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.security.auth.Subject;

import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.context.Flag;
import org.infinispan.rest.RequestHeader;
import org.infinispan.rest.framework.ContentSource;
import org.infinispan.rest.framework.FormParts;
import org.infinispan.rest.framework.Invocation;
import org.infinispan.rest.framework.Method;
import org.infinispan.rest.framework.RestRequest;
import org.infinispan.rest.framework.openapi.Parameter;
import org.infinispan.rest.framework.openapi.ParameterIn;

import io.netty.handler.codec.http.HttpHeaderNames;

public class ValidatingRestRequest implements RestRequest {
   private final RestRequest delegate;
   private final RestRequestValidator validator;

   public ValidatingRestRequest(RestRequest delegate, Invocation invocation) {
      this.delegate = delegate;
      this.validator = new RestRequestValidator(String.format("%s %s", delegate.method(), invocation.paths()), invocation);
   }

   @Override
   public Method method() {
      return delegate.method();
   }

   @Override
   public String path() {
      return delegate.path();
   }

   @Override
   public String uri() {
      return delegate.uri();
   }

   @Override
   public ContentSource contents() {
      validator.requireRestBody();
      return delegate.contents();
   }

   @Override
   public Map<String, List<String>> parameters() {
      return delegate.parameters();
   }

   @Override
   public String getParameter(String name) {
      validator.requireQueryParam(name);
      return delegate.getParameter(name);
   }

   @Override
   public Map<String, String> variables() {
      Map<String, String> variables = delegate.variables();
      if (variables == null)
         return null;
      return new ValidatingVariablesMap(variables, validator);
   }

   @Override
   public String getAction() {
      return delegate.getAction();
   }

   @Override
   public MediaType contentType() {
      return delegate.contentType();
   }

   @Override
   public MediaType keyContentType() {
      return delegate.keyContentType();
   }

   @Override
   public String getAcceptHeader() {
      return delegate.getAcceptHeader();
   }

   @Override
   public String getAuthorizationHeader() {
      return delegate.getAuthorizationHeader();
   }

   @Override
   public String getCacheControlHeader() {
      return delegate.getCacheControlHeader();
   }

   @Override
   public String getContentTypeHeader() {
      return delegate.getContentTypeHeader();
   }

   @Override
   public String getEtagIfMatchHeader() {
      validator.requireHeaderParam(HttpHeaderNames.IF_MATCH.toString());
      return delegate.getEtagIfMatchHeader();
   }

   @Override
   public String getIfModifiedSinceHeader() {
      validator.requireHeaderParam(HttpHeaderNames.IF_MODIFIED_SINCE.toString());
      return delegate.getIfModifiedSinceHeader();
   }

   @Override
   public String getEtagIfNoneMatchHeader() {
      validator.requireHeaderParam(RequestHeader.IF_NONE_MATCH.toString());
      return delegate.getEtagIfNoneMatchHeader();
   }

   @Override
   public String getIfUnmodifiedSinceHeader() {
      validator.requireHeaderParam(RequestHeader.IF_UNMODIFIED_SINCE.toString());
      return delegate.getIfUnmodifiedSinceHeader();
   }

   @Override
   public Long getMaxIdleTimeSecondsHeader() {
      validator.requireHeaderParam(RequestHeader.MAX_TIME_IDLE_HEADER.toString());
      return delegate.getMaxIdleTimeSecondsHeader();
   }

   @Override
   public Long getTimeToLiveSecondsHeader() {
      validator.requireHeaderParam(RequestHeader.TTL_SECONDS_HEADER.toString());
      return delegate.getTimeToLiveSecondsHeader();
   }

   @Override
   public EnumSet<CacheContainerAdmin.AdminFlag> getAdminFlags() {
      return delegate.getAdminFlags();
   }

   @Override
   public Flag[] getFlags() {
      return delegate.getFlags();
   }

   @Override
   public Long getCreatedHeader() {
      validator.requireHeaderParam(RequestHeader.CREATED_HEADER.toString());
      return delegate.getCreatedHeader();
   }

   @Override
   public Long getLastUsedHeader() {
      validator.requireHeaderParam(RequestHeader.LAST_USED_HEADER.toString());
      return delegate.getLastUsedHeader();
   }

   @Override
   public Subject getSubject() {
      return delegate.getSubject();
   }

   @Override
   public void setSubject(Subject subject) {
      delegate.setSubject(subject);
   }

   @Override
   public void setVariables(Map<String, String> variables) {
      delegate.setVariables(variables);
   }

   @Override
   public void setAction(String action) {
      delegate.setAction(action);
   }

   @Override
   public String header(String name) {
      validator.requireHeaderParam(name);
      return delegate.header(name);
   }

   @Override
   public List<String> headers(String name) {
      validator.requireHeaderParam(name);
      return delegate.headers(name);
   }

   @Override
   public Iterable<String> headersKeys() {
      return delegate.headersKeys();
   }

   @Override
   public InetSocketAddress getRemoteAddress() {
      return delegate.getRemoteAddress();
   }

   @Override
   public FormParts formData() {
      validator.requireRestBody();
      return delegate.formData();
   }

   private static final class RestRequestValidator {
      // Headers accessed by framework-level code (InvocationHelper, RestCacheManager),
      // not by handler business logic. Exempt from validation.
      private static final Set<String> EXEMPT_HEADERS = Set.of(
            RequestHeader.USER_AGENT.toString(),
            RequestHeader.FLAGS_HEADER.toString()
      );

      private final Set<String> queryParams;
      private final Set<String> pathParams;
      private final Set<String> headerParams;
      private final boolean hasRequestBody;
      private final String description;

      public RestRequestValidator(String description, Invocation invocation) {
         this.description = description;
         Collection<Parameter> params = invocation.parameters();
         this.queryParams = filterParams(params, ParameterIn.QUERY);
         this.pathParams = filterParams(params, ParameterIn.PATH);
         this.headerParams = filterParams(params, ParameterIn.HEADER);
         this.hasRequestBody = invocation.requestBody() != null;
      }

      private static Set<String> filterParams(Collection<Parameter> params, ParameterIn location) {
         if (params == null)
            return Set.of();

         return params.stream()
               .filter(p -> p.in() == location)
               .map(Parameter::name)
               .collect(Collectors.toSet());
      }

      public void requireQueryParam(String name) {
         if (!queryParams.contains(name)) {
            throw new UndeclaredParameterException("Query parameter", name, description);
         }
      }

      public void requireHeaderParam(String name) {
         if (EXEMPT_HEADERS.contains(name)) return;
         if (!headerParams.contains(name)) {
            throw new UndeclaredParameterException("Header", name, description);
         }
      }

      public void requirePathParam(String name) {
         if (!pathParams.contains(name)) {
            throw new UndeclaredParameterException("Path variable", name, description);
         }
      }

      public void requireRestBody() {
         if (!hasRequestBody)
            throw new UndeclaredParameterException("Request body accessed but not declared in OpenAPI schema for " + description);
      }
   }

   private static final class ValidatingVariablesMap implements Map<String, String> {
      private final Map<String, String> delegate;
      private final RestRequestValidator validator;

      private ValidatingVariablesMap(Map<String, String> delegate, RestRequestValidator validator) {
         this.delegate = delegate;
         this.validator = validator;
      }

      @Override
      public int size() {
         return delegate.size();
      }

      @Override
      public boolean isEmpty() {
         return delegate.isEmpty();
      }

      @Override
      public boolean containsKey(Object key) {
         return delegate.containsKey(key);
      }

      @Override
      public boolean containsValue(Object value) {
         return delegate.containsValue(value);
      }

      @Override
      public String get(Object key) {
         if (key instanceof String s) {
            validator.requirePathParam(s);
         }
         return delegate.get(key);
      }

      @Override
      public String put(String key, String value) {
         return delegate.put(key, value);
      }

      @Override
      public String remove(Object key) {
         return delegate.remove(key);
      }

      @Override
      public void putAll(Map<? extends String, ? extends String> m) {
         delegate.putAll(m);
      }

      @Override
      public void clear() {
         delegate.clear();
      }

      @Override
      public Set<String> keySet() {
         return delegate.keySet();
      }

      @Override
      public Collection<String> values() {
         return delegate.values();
      }

      @Override
      public Set<Entry<String, String>> entrySet() {
         return delegate.entrySet();
      }
   }
}
