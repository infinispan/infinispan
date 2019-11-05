package org.infinispan.rest;

import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

import java.io.File;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.rest.framework.RestResponse;
import org.infinispan.rest.framework.impl.RestResponseBuilder;

import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;

/**
 * A {@link RestResponse} backed by Netty.
 *
 * @since 10.0
 */
public class NettyRestResponse implements RestResponse {
   private final static String CACHE_CONTROL_HEADER = "Cache-Control";
   private final static String CLUSTER_PRIMARY_OWNER_HEADER = "Cluster-Primary-Owner";
   private final static String CLUSTER_BACKUP_OWNERS_HEADER = "Cluster-Backup-Owners";
   private final static String CLUSTER_NODE_NAME_HEADER = "Cluster-Node-Name";
   private final static String CLUSTER_SERVER_ADDRESS_HEADER = "Cluster-Server-Address";
   private final static String CONTENT_LENGTH_HEADER = "Content-Length";
   private final static String CONTENT_TYPE_HEADER = "Content-Type";
   private static final String CREATED_HEADER = "created";
   private final static String DATE_HEADER = "Date";
   private final static String ETAG_HEADER = "Etag";
   private final static String EXPIRES_HEADER = "Expires";
   private final static String LAST_MODIFIED_HEADER = "Last-Modified";
   private static final String LAST_USED_HEADER = "lastUsed";
   private final static String MAX_IDLE_TIME_HEADER = "maxIdleTimeSeconds";
   private final static String TIME_TO_LIVE_HEADER = "timeToLiveSeconds";
   private final static String WWW_AUTHENTICATE_HEADER = "WWW-Authenticate";
   private final HttpResponse response;
   private final Object entity;

   private NettyRestResponse(HttpResponse response, Object entity) {
      this.response = response;
      this.entity = entity;
   }

   public HttpResponse getResponse() {
      return response;
   }

   @Override
   public int getStatus() {
      return response.status().code();
   }

   @Override
   public Object getEntity() {
      return entity;
   }

   public static class Builder implements RestResponseBuilder<Builder> {
      private Map<String, List<String>> headers = new HashMap<>();
      private Object entity;
      private HttpResponseStatus httpStatus = OK;

      @Override
      public NettyRestResponse build() {
         HttpResponse response;
         if (entity instanceof File || entity instanceof InputStream) {
            response = new DefaultHttpResponse(HTTP_1_1, OK);
         } else {
            response = new DefaultFullHttpResponse(HTTP_1_1, OK, Unpooled.buffer());
         }
         response.setStatus(httpStatus);
         headers.forEach((name, values) -> response.headers().set(name, values));
         return new NettyRestResponse(response, entity);
      }

      @Override
      public Builder header(String name, Object value) {
         setHeader(name, value);
         return this;
      }

      public Builder status(HttpResponseStatus httpStatus) {
         this.httpStatus = httpStatus;
         return this;
      }

      @Override
      public Builder status(int status) {
         this.httpStatus = HttpResponseStatus.valueOf(status);
         return this;
      }

      @Override
      public Builder entity(Object entity) {
         this.entity = entity;
         return this;
      }

      @Override
      public Builder eTag(String tag) {
         setHeader(ETAG_HEADER, tag);
         return this;
      }

      @Override
      public int getStatus() {
         return httpStatus.code();
      }

      @Override
      public Object getEntity() {
         return entity;
      }

      @Override
      public Builder contentType(MediaType mediaType) {
         if (mediaType != null) {
            contentType(mediaType.toString());
         }
         return this;
      }

      @Override
      public Builder contentType(String mediaType) {
         setHeader(CONTENT_TYPE_HEADER, mediaType);
         return this;
      }

      @Override
      public Builder contentLength(long length) {
         setLongHeader(CONTENT_LENGTH_HEADER, length);
         return this;
      }

      @Override
      public Builder expires(Date expires) {
         if (expires != null) {
            setDateHeader(EXPIRES_HEADER, expires.getTime());
         }
         return this;
      }

      public Builder authenticate(String authentication) {
         if (authentication != null) {
            setHeader(WWW_AUTHENTICATE_HEADER, authentication);
         }
         return this;
      }

      @Override
      public Builder lastModified(Long epoch) {
         setDateHeader(LAST_MODIFIED_HEADER, epoch);
         return this;
      }

      @Override
      public Builder addProcessedDate(Date d) {
         if (d != null) {
            setDateHeader(DATE_HEADER, d.getTime());
         }
         return this;
      }

      @Override
      public Builder cacheControl(CacheControl cacheControl) {
         if (cacheControl != null) {
            setHeader(CACHE_CONTROL_HEADER, cacheControl.toString());
         }
         return this;
      }

      @Override
      public Object getHeader(String header) {
         return headers.get(header);
      }

      public Builder timeToLive(long timeToLive) {
         if (timeToLive > -1)
            setLongHeader(TIME_TO_LIVE_HEADER, TimeUnit.MILLISECONDS.toSeconds(timeToLive));
         return this;
      }

      public Builder maxIdle(long maxIdle) {
         if (maxIdle > -1)
            setLongHeader(MAX_IDLE_TIME_HEADER, TimeUnit.MILLISECONDS.toSeconds(maxIdle));
         return this;
      }

      public Builder created(long created) {
         if (created > -1) setHeader(CREATED_HEADER, String.valueOf(created));
         return this;
      }

      public Builder lastUsed(long lastUsed) {
         if (lastUsed > -1) setHeader(LAST_USED_HEADER, String.valueOf(lastUsed));
         return this;
      }

      public Builder clusterPrimaryOwner(String primaryOwner) {
         setHeader(CLUSTER_PRIMARY_OWNER_HEADER, primaryOwner);
         return this;
      }

      public Builder clusterBackupOwners(String primaryOwner) {
         setHeader(CLUSTER_BACKUP_OWNERS_HEADER, primaryOwner);
         return this;
      }

      public Builder clusterNodeName(String nodeName) {
         setHeader(CLUSTER_NODE_NAME_HEADER, nodeName);
         return this;
      }

      public Builder clusterServerAddress(String serverAddress) {
         setHeader(CLUSTER_SERVER_ADDRESS_HEADER, serverAddress);
         return this;
      }

      public HttpResponseStatus getHttpStatus() {
         return httpStatus;
      }

      private void setHeader(String name, Object value) {
         if (value != null) {
            headers.computeIfAbsent(name, a -> new ArrayList<>()).add(value.toString());
         }
      }

      private void setLongHeader(String name, long value) {
         headers.computeIfAbsent(name, a -> new ArrayList<>()).add(String.valueOf(value));
      }

      private void setDateHeader(String name, Long epoch) {
         if (epoch != null) {
            String value = DateUtils.toRFC1123(epoch);
            setHeader(name, value);
         }
      }
   }
}
