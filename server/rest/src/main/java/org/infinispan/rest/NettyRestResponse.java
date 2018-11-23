package org.infinispan.rest;

import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.rest.framework.RestResponse;
import org.infinispan.rest.framework.impl.RestResponseBuilder;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.AsciiString;

/**
 * A {@link RestResponse} backed by Netty.
 *
 * @since 10.0
 */
public class NettyRestResponse implements RestResponse {

   private final static DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.RFC_1123_DATE_TIME.withZone(ZoneId.systemDefault());
   private final static String TIME_TO_LIVE_HEADER = "timeToLiveSeconds";
   private final static String MAX_IDLE_TIME_HEADER = "maxIdleTimeSeconds";
   private final static String CLUSTER_PRIMARY_OWNER_HEADER = "Cluster-Primary-Owner";
   private final static String CLUSTER_NODE_NAME_HEADER = "Cluster-Node-Name";
   private final static String CLUSTER_SERVER_ADDRESS_HEADER = "Cluster-Server-Address";
   private final FullHttpResponse response;

   private NettyRestResponse(Builder builder) {
      response = builder.getResponse();
   }

   public FullHttpResponse getResponse() {
      return response;
   }

   @Override
   public int getStatus() {
      return response.status().code();
   }

   @Override
   public Object getEntity() {
      return response.content();
   }

   public static class Builder implements RestResponseBuilder<Builder> {
      private DefaultFullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, OK, Unpooled.buffer());
      private Object entity;

      @Override
      public NettyRestResponse build() {
         ByteBuf responseContent = response.content();
         if (entity instanceof String) {
            ByteBufUtil.writeUtf8(responseContent, entity.toString());
         } else if (entity instanceof byte[]) {
            responseContent.writeBytes((byte[]) entity);
         }
         HttpUtil.setContentLength(response, response.content().readableBytes());

         return new NettyRestResponse(this);
      }


      @Override
      public Builder header(String name, Object value) {
         response.headers().set(name, value);
         return this;
      }

      public Builder httpVersion(HttpVersion httpVersion) {
         response.setProtocolVersion(httpVersion);
         return this;
      }

      public Builder status(HttpResponseStatus httpStatus) {
         response.setStatus(httpStatus);
         return this;
      }

      @Override
      public Builder status(int status) {
         response.setStatus(HttpResponseStatus.valueOf(status));
         return this;
      }

      @Override
      public Builder entity(Object entity) {
         this.entity = entity;
         return this;
      }

      @Override
      public Builder eTag(String tag) {
         if (tag != null) response.headers().set(HttpHeaderNames.ETAG, tag);
         return this;
      }

      @Override
      public int getStatus() {
         return response.status().code();
      }

      @Override
      public Object getEntity() {
         return response.content();
      }


      @Override
      public Builder contentType(MediaType mediaType) {
         response.headers().set(HttpHeaderNames.CONTENT_TYPE, mediaType.toString());
         return this;
      }

      @Override
      public Builder expires(Date expires) {
         setDateHeader(HttpHeaderNames.EXPIRES, expires);
         return this;
      }

      public Builder authenticate(String authentication) {
         if (authentication != null) {
            response.headers().set(HttpHeaderNames.WWW_AUTHENTICATE, authentication);
         }
         return this;
      }

      @Override
      public Builder lastModified(Date lastModified) {
         setDateHeader(HttpHeaderNames.LAST_MODIFIED, lastModified);
         return this;
      }

      @Override
      public Builder cacheControl(CacheControl cacheControl) {
         if (cacheControl != null) {
            response.headers().set(HttpHeaderNames.CACHE_CONTROL, cacheControl);
         }
         return this;
      }

      @Override
      public Object getHeader(String header) {
         return response.headers().get(header);
      }

      public Builder timeToLive(long timeToLive) {
         if (timeToLive > -1) response.headers().set(TIME_TO_LIVE_HEADER, TimeUnit.MILLISECONDS.toSeconds(timeToLive));
         return this;
      }

      public Builder maxIdle(long maxIdle) {
         if (maxIdle > -1) response.headers().set(MAX_IDLE_TIME_HEADER, TimeUnit.MILLISECONDS.toSeconds(maxIdle));
         return this;
      }

      public Builder clusterPrimaryOwner(String primaryOwner) {
         response.headers().set(CLUSTER_PRIMARY_OWNER_HEADER, primaryOwner);
         return this;
      }

      public Builder clusterNodeName(String nodeName) {
         response.headers().set(CLUSTER_NODE_NAME_HEADER, nodeName);
         return this;
      }

      public Builder clusterServerAddress(String serverAddress) {
         response.headers().set(CLUSTER_SERVER_ADDRESS_HEADER, serverAddress);
         return this;
      }

      public HttpVersion getHttpVersion() {
         return response.protocolVersion();
      }

      public HttpResponseStatus getHttpStatus() {
         return response.status();
      }

      public DefaultFullHttpResponse getResponse() {
         return response;
      }

      private void setDateHeader(AsciiString headerName, Date value) {
         if (value != null) {
            response.headers().set(headerName, DATE_TIME_FORMATTER.format(value.toInstant()));
         }
      }
   }
}
