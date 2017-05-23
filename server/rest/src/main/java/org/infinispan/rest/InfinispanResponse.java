package org.infinispan.rest;

import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.infinispan.rest.operations.mediatypes.Charset;
import org.infinispan.rest.operations.mediatypes.MediaType;

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

/**
 * Representation of a HTTP response tailed for Infinispan-specific responses.
 *
 * @author Sebastian Łaskawiec
 */
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

   /**
    * Creates an {@link InfinispanResponse} as a reply to specific {@link InfinispanRequest}.
    *
    * @param request Request to reply to.
    * @return Response object.
    */
   public static final InfinispanResponse inReplyTo(InfinispanRequest request) {
      return new InfinispanResponse(Optional.of(request));
   }

   /**
    * Creates an {@link InfinispanResponse} as an error.
    *
    * @return Response object.
    */
   public static final InfinispanResponse asError(InfinispanRequest request, HttpResponseStatus status, String description) {
      InfinispanResponse infinispanResponse = new InfinispanResponse(Optional.of(request));
      infinispanResponse.status(status);
      if (description != null) {
         infinispanResponse.contentAsText(description);
      }
      return infinispanResponse;
   }

   /**
    * Adds content as text. Converts it internally to <code>UTF-8</code>.
    *
    * @param content Content.
    */
   public void contentAsText(String content) {
      ByteBufUtil.writeUtf8(this.content, content);
   }

   /**
    * Adds content as binary array.
    *
    * @param content Content.
    */
   public void contentAsBytes(byte[] content) {
      this.content.writeBytes(content);
   }

   /**
    * Adds <code>Content-Type</code> header.
    *
    * @param contentType <code>Content-Type</code> header.
    */
   public void contentType(String contentType) {
      this.contentType = contentType;
   }

   /**
    * Adds HTTP version header.
    *
    * @param httpVersion HTTP version header.
    */
   public void httpVersion(HttpVersion httpVersion) {
      this.httpVersion = httpVersion;
   }

   /**
    * Adds status code.
    *
    * @param httpStatus Status code.
    */
   public void status(HttpResponseStatus httpStatus) {
      this.httpStatus = httpStatus;
   }

   /**
    * Checks whether this is a Keep Alive type of response.
    *
    * @return <code>true</code> if the response contains Keep Alive headers.
    */
   public boolean isKeepAlive() {
      boolean isKeepAlive = request.map(r -> HttpUtil.isKeepAlive(r.getRawRequest())).orElse(false);
      return (httpVersion == HttpVersion.HTTP_1_1 || httpVersion == HttpVersion.HTTP_1_0) && isKeepAlive;
   }

   /**
    * Renders {@link FullHttpResponse} object.
    *
    * @return {@link FullHttpResponse} object based on this {@link InfinispanResponse}.
    */
   public FullHttpResponse toNettyHttpResponse() {
      FullHttpResponse response = new DefaultFullHttpResponse(httpVersion, httpStatus, content);

      request.flatMap(InfinispanRequest::getStreamId)
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

   /**
    * Adds ETAG.
    *
    * @param etag ETag value to be added.
    */
   public void etag(String etag) {
      this.etag = Optional.of(etag);
   }

   /**
    * Adds Cache Control headers.
    *
    * @param cacheControl Cache Control headers.
    */
   public void cacheControl(CacheControl cacheControl) {
      this.cacheControl = Optional.ofNullable(cacheControl);
   }

   /**
    * Add <code>last-modified</code> header.
    *
    * @param lastModified <code>last-modified</code> header value.
    */
   public void lastModified(Date lastModified) {
      this.lastModified = Optional.ofNullable(lastModified);
   }

   /**
    * Adds <code>expires</code> header.
    *
    * @param expires <code>expires</code> header value.
    */
   public void expires(Date expires) {
      this.expires = Optional.ofNullable(expires);
   }

   public void timeToLive(long lifespan) {
      if(lifespan > -1) {
         this.timeToLive = Optional.of(Long.valueOf(lifespan));
      }
   }

   /**
    * Adds <code>maxIdleTimeSeconds</code> header.
    * @param maxIdle <code>maxIdleTimeSeconds</code> header value.
    */
   public void maxIdle(long maxIdle) {
      if(maxIdle > -1) {
         this.maxIdle = Optional.of(Long.valueOf(maxIdle));
      }
   }

   /**
    * Adds <code>Cluster-Primary-Owner</code> header.
    *
    * @param primaryOwner <code>Cluster-Primary-Owner</code> header value.
    */
   public void clusterPrimaryOwner(String primaryOwner) {
      this.clusterPrimaryOwner = Optional.of(primaryOwner);
   }

   /**
    * Adds <code>Cluster-Node-Name</code> header.
    *
    * @param nodeName <code>Cluster-Node-Name</code> header value.
    */
   public void clusterNodeName(String nodeName) {
      this.clusterNodeName = Optional.of(nodeName);
   }

   /**
    * Adds <code>Cluster-Server-Address</code> header.
    *
    * @param serverAddress <code>Cluster-Server-Address</code> header value.
    */
   public void clusterServerAddress(String serverAddress) {
      this.clusterServerAddress = Optional.of(serverAddress);
   }

   /**
    * Adds <code>www-authenticate</code> header.
    * @param authenticateHeader <code>www-authenticate</code> header value.
    */
   public void authenticate(String authenticateHeader) {
      this.authenticate = Optional.of(authenticateHeader);
   }

   /**
    * Adds a charset.
    * @param charset charset value.
    */
   public void charset(Charset charset) {
      this.charset = Optional.ofNullable(charset);
   }

}
