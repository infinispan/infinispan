package org.infinispan.rest.embedded.netty4;

import static io.netty.handler.codec.http.HttpHeaders.is100ContinueExpected;

import java.util.List;

import org.infinispan.rest.embedded.netty4.i18n.LogMessages;
import org.infinispan.rest.embedded.netty4.i18n.Messages;
import org.jboss.resteasy.core.SynchronousDispatcher;
import org.jboss.resteasy.specimpl.ResteasyHttpHeaders;
import org.jboss.resteasy.spi.ResteasyUriInfo;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaders;

/**
 * This {@link MessageToMessageDecoder} is responsible for decode {@link io.netty.handler.codec.http.HttpRequest}
 * to {@link NettyHttpRequest}'s
 * <p>
 * This implementation is {@link Sharable}
 *
 * @author Norman Maurer
 * Temporary fork from RestEasy 3.1.0
 */
@Sharable
public class RestEasyHttpRequestDecoder extends MessageToMessageDecoder<io.netty.handler.codec.http.HttpRequest> {
   private final SynchronousDispatcher dispatcher;
   private final String servletMappingPrefix;
   private final String proto;

   public RestEasyHttpRequestDecoder(SynchronousDispatcher dispatcher, String servletMappingPrefix, Protocol protocol) {
      this.dispatcher = dispatcher;
      this.servletMappingPrefix = servletMappingPrefix;
      if (protocol == Protocol.HTTP) {
         proto = "http";
      } else {
         proto = "https";
      }
   }

   @Override
   protected void decode(ChannelHandlerContext ctx, io.netty.handler.codec.http.HttpRequest request, List<Object> out) throws Exception {
      boolean keepAlive = HttpHeaders.isKeepAlive(request);
      final NettyHttpResponse response = new NettyHttpResponse(ctx, keepAlive, dispatcher.getProviderFactory());
      final ResteasyHttpHeaders headers;
      final ResteasyUriInfo uriInfo;
      try {
         headers = NettyUtil.extractHttpHeaders(request);

         uriInfo = NettyUtil.extractUriInfo(request, servletMappingPrefix, proto);
         NettyHttpRequest nettyRequest = new NettyHttpRequest(ctx, headers, uriInfo, request.getMethod().name(), dispatcher, response, is100ContinueExpected(request));
         if (request instanceof HttpContent) {
            HttpContent content = (HttpContent) request;
            ByteBuf byteBuf = content.content();

            // Does the request contain a body that will need to be retained
            if (byteBuf.readableBytes() > 0) {
               ByteBuf buf = byteBuf.retain();
               nettyRequest.setContentBuffer(buf);
            }

            out.add(nettyRequest);
         }
      } catch (Exception e) {
         response.sendError(400);
         // made it warn so that people can filter this.
         LogMessages.LOGGER.warn(Messages.MESSAGES.failedToParseRequest(), e);
      }
   }

   public enum Protocol {
      HTTPS,
      HTTP
   }
}
