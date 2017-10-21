package org.infinispan.rest;

import java.util.Optional;

import org.infinispan.rest.operations.mediatypes.Charset;

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
public abstract class InfinispanResponse {

   private Optional<InfinispanRequest> request;
   private ByteBuf content = Unpooled.buffer();
   private String contentType;
   private HttpVersion httpVersion = HttpVersion.HTTP_1_1;
   private HttpResponseStatus httpStatus = HttpResponseStatus.OK;
   private Optional<String> authenticate = Optional.empty();
   private Optional<Charset> charset = Optional.empty();

   protected InfinispanResponse(Optional<InfinispanRequest> request) {
      this.request = request;
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

   protected void addSpecificHeaders(FullHttpResponse response) {
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

      addSpecificHeaders(response);

      authenticate.ifPresent(e -> response.headers().set(HttpHeaderNames.WWW_AUTHENTICATE, e));

      if (contentType != null) {
         StringBuilder contentTypeWithCharset = new StringBuilder(contentType);
         charset.ifPresent(charset -> contentTypeWithCharset.append(';').append(charset));
         response.headers().set(HttpHeaderNames.CONTENT_TYPE, contentTypeWithCharset.toString());
      }
      HttpUtil.setContentLength(response, response.content().readableBytes());
      return response;
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
