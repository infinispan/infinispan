package org.infinispan.rest;

import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static org.infinispan.rest.ResponseHeader.CLUSTER_BACKUP_OWNERS_HEADER;
import static org.infinispan.rest.ResponseHeader.CLUSTER_NODE_NAME_HEADER;
import static org.infinispan.rest.ResponseHeader.CLUSTER_PRIMARY_OWNER_HEADER;
import static org.infinispan.rest.ResponseHeader.CLUSTER_SERVER_ADDRESS_HEADER;
import static org.infinispan.rest.ResponseHeader.CONTENT_LENGTH_HEADER;
import static org.infinispan.rest.ResponseHeader.CONTENT_TYPE_HEADER;
import static org.infinispan.rest.ResponseHeader.CREATED_HEADER;
import static org.infinispan.rest.ResponseHeader.DATE_HEADER;
import static org.infinispan.rest.ResponseHeader.ETAG_HEADER;
import static org.infinispan.rest.ResponseHeader.EXPIRES_HEADER;
import static org.infinispan.rest.ResponseHeader.LAST_MODIFIED_HEADER;
import static org.infinispan.rest.ResponseHeader.LAST_USED_HEADER;
import static org.infinispan.rest.ResponseHeader.LOCATION;
import static org.infinispan.rest.ResponseHeader.MAX_IDLE_TIME_HEADER;
import static org.infinispan.rest.ResponseHeader.TIME_TO_LIVE_HEADER;
import static org.infinispan.rest.ResponseHeader.WWW_AUTHENTICATE_HEADER;

import java.io.File;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.rest.framework.RestResponse;
import org.infinispan.rest.framework.impl.RestResponseBuilder;
import org.infinispan.rest.stream.CacheChunkedStream;

import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.stream.ChunkedInput;

/**
 * A {@link RestResponse} backed by Netty.
 *
 * @since 10.0
 */
public class NettyRestResponse implements RestResponse {
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

      public Builder() {}

      @Override
      public NettyRestResponse build() {
         HttpResponse response;
         if (entity instanceof File || entity instanceof ChunkedInput || entity instanceof EventStream ||
               entity instanceof CacheChunkedStream) {
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
         setHeader(ETAG_HEADER.getValue(), tag);
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
         setHeader(CONTENT_TYPE_HEADER.getValue(), mediaType);
         return this;
      }

      @Override
      public Builder contentLength(long length) {
         setLongHeader(CONTENT_LENGTH_HEADER.getValue(), length);
         return this;
      }

      @Override
      public Builder expires(Date expires) {
         if (expires != null) {
            setDateHeader(EXPIRES_HEADER.getValue(), expires.getTime());
         }
         return this;
      }

      public Builder authenticate(String authentication) {
         if (authentication != null) {
            setHeader(WWW_AUTHENTICATE_HEADER.getValue(), authentication);
         }
         return this;
      }

      @Override
      public Builder lastModified(Long epoch) {
         setDateHeader(LAST_MODIFIED_HEADER.getValue(), epoch);
         return this;
      }

      @Override
      public Builder location(String location) {
         setHeader(LOCATION.getValue(), location);
         return this;
      }

      @Override
      public Builder addProcessedDate(Date d) {
         if (d != null) {
            setDateHeader(DATE_HEADER.getValue(), d.getTime());
         }
         return this;
      }

      @Override
      public Builder cacheControl(CacheControl cacheControl) {
         if (cacheControl != null) {
            setHeader(ResponseHeader.CACHE_CONTROL_HEADER.getValue(), cacheControl.toString());
         }
         return this;
      }

      @Override
      public Object getHeader(String header) {
         return headers.get(header);
      }

      public Builder timeToLive(long timeToLive) {
         if (timeToLive > -1)
            setLongHeader(TIME_TO_LIVE_HEADER.getValue(), TimeUnit.MILLISECONDS.toSeconds(timeToLive));
         return this;
      }

      public Builder maxIdle(long maxIdle) {
         if (maxIdle > -1)
            setLongHeader(MAX_IDLE_TIME_HEADER.getValue(), TimeUnit.MILLISECONDS.toSeconds(maxIdle));
         return this;
      }

      public Builder created(long created) {
         if (created > -1) setHeader(CREATED_HEADER.getValue(), String.valueOf(created));
         return this;
      }

      public Builder lastUsed(long lastUsed) {
         if (lastUsed > -1) setHeader(LAST_USED_HEADER.getValue(), String.valueOf(lastUsed));
         return this;
      }

      public Builder clusterPrimaryOwner(String primaryOwner) {
         setHeader(CLUSTER_PRIMARY_OWNER_HEADER.getValue(), primaryOwner);
         return this;
      }

      public Builder clusterBackupOwners(String primaryOwner) {
         setHeader(CLUSTER_BACKUP_OWNERS_HEADER.getValue(), primaryOwner);
         return this;
      }

      public Builder clusterNodeName(String nodeName) {
         setHeader(CLUSTER_NODE_NAME_HEADER.getValue(), nodeName);
         return this;
      }

      public Builder clusterServerAddress(String serverAddress) {
         setHeader(CLUSTER_SERVER_ADDRESS_HEADER.getValue(), serverAddress);
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
