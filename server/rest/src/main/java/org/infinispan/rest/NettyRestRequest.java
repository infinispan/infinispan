package org.infinispan.rest;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

import javax.security.auth.Subject;

import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.context.Flag;
import org.infinispan.rest.framework.ContentSource;
import org.infinispan.rest.framework.Method;
import org.infinispan.rest.framework.RestRequest;
import org.infinispan.rest.logging.Log;
import org.infinispan.rest.operations.exceptions.InvalidFlagException;
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

   private static final MediaType DEFAULT_KEY_CONTENT_TYPE = MediaType.fromString("application/x-java-object;type=java.lang.String");

   public static final String EXTENDED_HEADER = "extended";
   private static final String MAX_TIME_IDLE_HEADER = "maxIdleTimeSeconds";
   private static final String CREATED_HEADER = "created";
   private static final String LAST_USED_HEADER = "lastUsed";
   private static final String TTL_SECONDS_HEADER = "timeToLiveSeconds";
   private static final String KEY_CONTENT_TYPE_HEADER = "key-content-type";
   private static final String FLAGS_HEADER = "flags";

   private final FullHttpRequest request;
   private final Map<String, List<String>> parameters;
   private final String path;
   private final ContentSource contentSource;
   private final String context;
   private String action;
   private Subject subject;
   private Map<String, String> variables;

   private String getPath(String uri) {
      int lastSeparatorIdx = -1;
      int paramsSeparatorIdx = -1;
      for (int i = 0; i < uri.length(); i++) {
         char c = uri.charAt(i);
         if (c == '/') lastSeparatorIdx = i;
         if (c == '?') paramsSeparatorIdx = i;
      }
      String baseURI = uri.substring(0, lastSeparatorIdx);
      String resourceName = uri.substring(lastSeparatorIdx + 1, paramsSeparatorIdx != -1 ? paramsSeparatorIdx : uri.length());
      String decodedPath = QueryStringDecoder.decodeComponent(baseURI);
      return decodedPath + "/" + resourceName;
   }

   NettyRestRequest(FullHttpRequest request) throws IllegalArgumentException {
      this.request = request;
      String uri = request.uri();
      QueryStringDecoder queryStringDecoder = new QueryStringDecoder(uri);
      this.parameters = queryStringDecoder.parameters();
      this.path = getPath(uri);
      this.context = getContext(this.path);
      List<String> action = queryStringDecoder.parameters().get("action");
      if (action != null) {
         this.action = action.iterator().next();
      }
      this.contentSource = new ByteBufContentSource(request.content());
   }

   private String getContext(String path) {
      if (path == null || path.isEmpty() || !path.startsWith("/") || path.length() == 1) return "";
      int endIndex = path.indexOf("/", 1);
      return path.substring(1, endIndex == -1 ? path.length() : endIndex);
   }

   public String getContext() {
      return context;
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
   public String uri() {
      return request.uri();
   }

   @Override
   public String header(String name) {
      return request.headers().get(name);
   }

   @Override
   public List<String> headers(String name) {
      return request.headers().getAll(name);
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
   public String getParameter(String name) {
      if (parameters == null || !parameters.containsKey(name)) return null;
      List<String> values = parameters.get(name);
      return values.get(values.size() - 1);
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
      return MediaType.fromString(contentTypeHeader);
   }

   @Override
   public MediaType keyContentType() {
      String header = request.headers().get(KEY_CONTENT_TYPE_HEADER);
      if (header == null) return DEFAULT_KEY_CONTENT_TYPE;
      return MediaType.fromString(header);
   }

   @Override
   public String getAcceptHeader() {
      String accept = request.headers().get(HttpHeaderNames.ACCEPT);
      return accept == null ? MediaType.MATCH_ALL_TYPE : accept;
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
   public String getIfModifiedSinceHeader() {
      return request.headers().get(HttpHeaderNames.IF_MODIFIED_SINCE);
   }

   @Override
   public String getEtagIfNoneMatchHeader() {
      return request.headers().get(HttpHeaderNames.IF_NONE_MATCH);
   }

   @Override
   public String getIfUnmodifiedSinceHeader() {
      return request.headers().get(HttpHeaderNames.IF_UNMODIFIED_SINCE);
   }

   @Override
   public Long getMaxIdleTimeSecondsHeader() {
      return getHeaderAsLong(MAX_TIME_IDLE_HEADER);
   }


   @Override
   public Long getTimeToLiveSecondsHeader() {
      return getHeaderAsLong(TTL_SECONDS_HEADER);
   }

   @Override
   public EnumSet<CacheContainerAdmin.AdminFlag> getAdminFlags() {
      String requestFlags = request.headers().get(FLAGS_HEADER);
      if (requestFlags == null || requestFlags.isEmpty()) return null;
      try {
         return CacheContainerAdmin.AdminFlag.fromString(requestFlags);
      } catch (IllegalArgumentException e) {
         throw new InvalidFlagException(e);
      }
   }

   @Override
   public Flag[] getFlags() {
      try {
         String flags = request.headers().get(FLAGS_HEADER);
         if (flags == null || flags.isEmpty()) {
            return null;
         }
         return Arrays.stream(flags.split(",")).filter(s -> !s.isEmpty()).map(Flag::valueOf).toArray(Flag[]::new);
      } catch (IllegalArgumentException e) {
         throw new InvalidFlagException(e);
      }
   }

   @Override
   public Long getCreatedHeader() {
      return getHeaderAsLong(CREATED_HEADER);
   }

   @Override
   public Long getLastUsedHeader() {
      return getHeaderAsLong(LAST_USED_HEADER);
   }

   public Subject getSubject() {
      return subject;
   }

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

   public FullHttpRequest getFullHttpRequest() {
      return request;
   }

   @Override
   public String toString() {
      return "NettyRestRequest{" +
            request.method().name() +
            " " + request.uri() +
            ", subject=" + subject +
            '}';
   }

}
