package org.infinispan.rest;

import java.security.Principal;
import java.util.List;
import java.util.Map;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.rest.framework.ContentSource;
import org.infinispan.rest.framework.Method;
import org.infinispan.rest.framework.RestRequest;
import org.infinispan.rest.logging.Log;
import org.infinispan.util.logging.LogFactory;

import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.QueryStringDecoder;

/**
 * A {@link RestRequest} backed by Netty.
 *
 * @since 10.0
 */
public class NettyRestRequest implements RestRequest {

   private final static Log logger = LogFactory.getLog(NettyRestRequest.class, Log.class);

   private static final MediaType DEFAULT_KEY_CONTENT_TYPE = MediaType.parse("application/x-java-object;type=java.lang.String");

   public static final String EXTENDED_HEADER = "extended";
   private static final String MAX_TIME_IDLE_HEADER = "maxIdleTimeSeconds";
   private static final String CREATED_HEADER = "created";
   private static final String LAST_USED_HEADER = "lastUsed";
   private static final String TTL_SECONDS_HEADER = "timeToLiveSeconds";
   private static final String PERFORM_ASYNC_HEADER = "performAsync";
   private static final String KEY_CONTENT_TYPE_HEADER = "key-content-type";

   private final FullHttpRequest request;
   private final Map<String, List<String>> parameters;
   private final String path;
   private final ContentSource contentSource;
   private String action;
   private Principal principal;
   private Map<String, String> variables;

   NettyRestRequest(FullHttpRequest request) {
      this.request = request;
      QueryStringDecoder queryStringDecoder = new QueryStringDecoder(request.uri());
      this.parameters = queryStringDecoder.parameters();
      this.path = queryStringDecoder.path();
      List<String> action = queryStringDecoder.parameters().get("action");
      if (action != null) {
         this.action = action.iterator().next();
      }
      this.contentSource = new ByteBufContentSource(request.content());
   }

   @Override
   public Method method() {
      return Method.valueOf(request.method().name());
   }

   @Override
   public String path() {
      return path;
   }

   @Override
   public ContentSource contents() {
      return contentSource;
   }

   @Override
   public Map<String, List<String>> parameters() {
      return parameters;
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
      String contentTypeHeader = getContentTypeHeader();
      if (contentTypeHeader == null) return MediaType.MATCH_ALL;
      return MediaType.parse(contentTypeHeader);
   }

   @Override
   public MediaType keyContentType() {
      String header = request.headers().get(KEY_CONTENT_TYPE_HEADER);
      if (header == null) return DEFAULT_KEY_CONTENT_TYPE;
      return MediaType.parse(header);
   }

   @Override
   public String getAcceptHeader() {
      return request.headers().get(HttpHeaderNames.ACCEPT);
   }

   @Override
   public String getAuthorizationHeader() {
      return request.headers().get(HttpHeaderNames.AUTHORIZATION);
   }

   @Override
   public String getCacheControlHeader() {
      String value = request.headers().get(HttpHeaderNames.CACHE_CONTROL);
      if (value == null) return "";
      return value;
   }

   @Override
   public String getContentTypeHeader() {
      return request.headers().get(HttpHeaderNames.CONTENT_TYPE);
   }

   @Override
   public String getEtagIfMatchHeader() {
      return request.headers().get(HttpHeaderNames.IF_MATCH);
   }

   @Override
   public String getEtagIfModifiedSinceHeader() {
      return request.headers().get(HttpHeaderNames.IF_MODIFIED_SINCE);
   }

   @Override
   public String getEtagIfNoneMatchHeader() {
      return request.headers().get(HttpHeaderNames.IF_NONE_MATCH);
   }

   @Override
   public String getEtagIfUnmodifiedSinceHeader() {
      return request.headers().get(HttpHeaderNames.IF_UNMODIFIED_SINCE);
   }

   @Override
   public Long getMaxIdleTimeSecondsHeader() {
      return getHeaderAsLong(MAX_TIME_IDLE_HEADER);
   }

   @Override
   public boolean getPerformAsyncHeader() {
      return getHeaderAsBoolean(PERFORM_ASYNC_HEADER);
   }

   @Override
   public Long getTimeToLiveSecondsHeader() {
      return getHeaderAsLong(TTL_SECONDS_HEADER);
   }

   @Override
   public Long getCreatedHeader() {
      return getHeaderAsLong(CREATED_HEADER);
   }

   @Override
   public Long getLastUsedHeader() {
      return getHeaderAsLong(LAST_USED_HEADER);
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

   private boolean getHeaderAsBoolean(String header) {
      String headerValue = request.headers().get(header);
      if (header == null) return false;
      return Boolean.valueOf(headerValue);
   }

   private Long getHeaderAsLong(String header) {
      String headerValue = request.headers().get(header);
      if (headerValue == null) return null;
      try {
         return Long.valueOf(headerValue);
      } catch (NumberFormatException e) {
         logger.warnInvalidNumber(header, headerValue);
         return null;
      }
   }
}
