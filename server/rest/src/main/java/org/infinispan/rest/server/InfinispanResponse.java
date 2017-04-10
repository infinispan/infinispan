package org.infinispan.rest.server;

import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.infinispan.rest.server.operations.mediatypes.Charset;
import org.infinispan.rest.server.operations.mediatypes.MediaType;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http2.HttpConversionUtil;

public class InfinispanResponse {

   private final static DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.RFC_1123_DATE_TIME.withZone(ZoneId.systemDefault());
   private final static String TIME_TO_LIVE_HEADER = "timeToLiveSeconds";
   private final static String MAX_IDLE_TIME_HEADER = "maxIdleTimeSeconds";
   private final static String CLUSTER_PRIMARY_OWNER_HEADER = "Cluster-Primary-Owner";
   private final static String CLUSTER_NODE_NAME_HEADER = "Cluster-Node-Name";
   private final static String CLUSTER_SERVER_ADDRESS_HEADER = "Cluster-Server-Address";

   private Optional<InfinispanRequest> request;
   private ByteBuf content = Unpooled.buffer();
   private String contentType = MediaType.TEXT_PLAIN.toString();
   private HttpVersion httpVersion = HttpVersion.HTTP_1_1;
   private HttpResponseStatus httpStatus = HttpResponseStatus.OK;
   private Optional<String> etag = Optional.empty();
   private Optional<CacheControl> cacheControl = Optional.empty();
   private Optional<Date> lastModified = Optional.empty();
   private Optional<Date> expires = Optional.empty();
   private Optional<Long> timeToLive = Optional.empty();
   private Optional<Long> maxIdle = Optional.empty();
   private Optional<String> clusterPrimaryOwner = Optional.empty();
   private Optional<String> clusterNodeName = Optional.empty();
   private Optional<String> clusterServerAddress = Optional.empty();
   private Optional<String> authenticate = Optional.empty();
   private Optional<Charset> charset = Optional.empty();

   private InfinispanResponse(Optional<InfinispanRequest> request) {
      this.request = request;
   }

   public static final InfinispanResponse inReplyTo(InfinispanRequest request) {
      return new InfinispanResponse(Optional.of(request));
   }

   public static final InfinispanResponse asError(HttpResponseStatus status, String description) {
      InfinispanResponse infinispanResponse = new InfinispanResponse(Optional.empty());
      infinispanResponse.status(status);
      infinispanResponse.contentAsText(description);
      return infinispanResponse;
   }

   public void contentAsText(String content) {
      ByteBufUtil.writeUtf8(this.content, content);
   }

   public void contentAsBytes(byte[] content) {
      this.content.writeBytes(content);
   }


   public void contentType(String contentType) {
      this.contentType = contentType;
   }

   public void httpVersion(HttpVersion httpVersion) {
      this.httpVersion = httpVersion;
   }

   public void status(HttpResponseStatus httpStatus) {
      this.httpStatus = httpStatus;
   }

   public boolean isKeepAlive() {
      boolean isKeepAlive = request.map(r -> HttpUtil.isKeepAlive(r.getRawRequest())).orElse(false);
      return (httpVersion == HttpVersion.HTTP_1_1 || httpVersion == HttpVersion.HTTP_1_0) && isKeepAlive;
   }

   public FullHttpResponse toNettyHttpResponse() {
      FullHttpResponse response = new DefaultFullHttpResponse(httpVersion, httpStatus, content);

      request
            .flatMap(InfinispanRequest::getStreamId)
            .ifPresent(streamId -> response.headers().set(HttpConversionUtil.ExtensionHeaderNames.STREAM_ID.text(), streamId));

      if (isKeepAlive()) {
         response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
      }

      etag.ifPresent(e -> response.headers().set(HttpHeaderNames.ETAG, e));
      cacheControl.ifPresent(e -> response.headers().set(HttpHeaderNames.CACHE_CONTROL, e));
      lastModified.ifPresent(e -> response.headers().set(HttpHeaderNames.LAST_MODIFIED, DATE_TIME_FORMATTER.format(e.toInstant())));
      expires.ifPresent(e -> response.headers().set(HttpHeaderNames.EXPIRES, DATE_TIME_FORMATTER.format(e.toInstant())));
      timeToLive.ifPresent(e -> response.headers().set(TIME_TO_LIVE_HEADER, TimeUnit.MILLISECONDS.toSeconds(e)));
      maxIdle.ifPresent(e -> response.headers().set(MAX_IDLE_TIME_HEADER, TimeUnit.MILLISECONDS.toSeconds(e)));
      clusterPrimaryOwner.ifPresent(e -> response.headers().set(CLUSTER_PRIMARY_OWNER_HEADER, e));
      clusterNodeName.ifPresent(e -> response.headers().set(CLUSTER_NODE_NAME_HEADER, e));
      clusterServerAddress.ifPresent(e -> response.headers().set(CLUSTER_SERVER_ADDRESS_HEADER, e));
      authenticate.ifPresent(e -> response.headers().set(HttpHeaderNames.WWW_AUTHENTICATE, e));

      StringBuilder contentTypeWithCharset = new StringBuilder(contentType);
      if (charset.isPresent()) {
         contentTypeWithCharset.append(';').append(charset.get());
      }
      response.headers().set(HttpHeaderNames.CONTENT_TYPE, contentTypeWithCharset.toString());
      HttpUtil.setContentLength(response, response.content().readableBytes());
      return response;
   }

   public void etag(String etag) {
      this.etag = Optional.of(etag);
   }

   public void cacheControl(CacheControl cacheControl) {
      this.cacheControl = Optional.ofNullable(cacheControl);
   }

   public void lastModified(Date lastModified) {
      this.lastModified = Optional.ofNullable(lastModified);
   }

   public void expires(Date expires) {
      this.expires = Optional.ofNullable(expires);
   }

   public void timeToLive(long lifespan) {
      if(lifespan > -1) {
         this.timeToLive = Optional.of(Long.valueOf(lifespan));
      }
   }

   public void maxIdle(long maxIdle) {
      if(maxIdle > -1) {
         this.maxIdle = Optional.of(Long.valueOf(maxIdle));
      }
   }

   public void clusterPrimaryOwner(String primaryOwner) {
      this.clusterPrimaryOwner = Optional.of(primaryOwner);
   }

   public void clusterNodeName(String nodeName) {
      this.clusterNodeName = Optional.of(nodeName);
   }

   public void clusterServerAddress(String serverAddress) {
      this.clusterServerAddress = Optional.of(serverAddress);
   }

   public void authenticate(String authenticateHeader) {
      this.authenticate = Optional.of(authenticateHeader);
   }

   public void charset(Charset charset) {
      this.charset = Optional.ofNullable(charset);
   }
}
