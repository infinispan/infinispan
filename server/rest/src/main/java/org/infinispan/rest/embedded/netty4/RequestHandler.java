package org.infinispan.rest.embedded.netty4;

import static io.netty.handler.codec.http.HttpResponseStatus.CONTINUE;
import static io.netty.handler.codec.http.HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

import org.infinispan.rest.embedded.netty4.i18n.LogMessages;
import org.infinispan.rest.embedded.netty4.i18n.Messages;
import org.jboss.resteasy.spi.Failure;

import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.TooLongFrameException;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.timeout.IdleStateEvent;

/**
 * {@link SimpleChannelInboundHandler} which handles the requests and dispatch them.
 * <p>
 * This class is {@link Sharable}.
 *
 * @author <a href="http://www.jboss.org/netty/">The Netty Project</a>
 * @author Andy Taylor (andy.taylor@jboss.org)
 * @author <a href="http://gleamynode.net/">Trustin Lee</a>
 * @author Norman Maurer
 * @version $Rev: 2368 $, $Date: 2010-10-18 17:19:03 +0900 (Mon, 18 Oct 2010) $
 * Temporary fork from RestEasy 3.1.0
 */
@Sharable
public class RequestHandler extends SimpleChannelInboundHandler {
   protected final RequestDispatcher dispatcher;

   public RequestHandler(RequestDispatcher dispatcher) {
      this.dispatcher = dispatcher;
   }

   @Override
   protected void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
      if (msg instanceof NettyHttpRequest) {
         NettyHttpRequest request = (NettyHttpRequest) msg;
         try {

            if (request.is100ContinueExpected()) {
               send100Continue(ctx);
            }

            NettyHttpResponse response = request.getResponse();
            try {
               dispatcher.service(ctx, request, response, true);
            } catch (Failure e1) {
               response.reset();
               response.setStatus(e1.getErrorCode());
            } catch (Exception ex) {
               response.reset();
               response.setStatus(500);
               LogMessages.LOGGER.error(Messages.MESSAGES.unexpected(), ex);
            }

            if (!request.getAsyncContext().isSuspended()) {
               response.finish();
            }
         } finally {
            request.releaseContentBuffer();
         }

      }
   }

   private void send100Continue(ChannelHandlerContext ctx) {
      HttpResponse response = new DefaultHttpResponse(HTTP_1_1, CONTINUE);
      ctx.writeAndFlush(response);
   }

   @Override
   public void exceptionCaught(ChannelHandlerContext ctx, Throwable e)
         throws Exception {
      // handle the case of to big requests.
      if (e.getCause() instanceof TooLongFrameException) {
         DefaultHttpResponse response = new DefaultHttpResponse(HTTP_1_1, REQUEST_ENTITY_TOO_LARGE);
         ctx.write(response).addListener(ChannelFutureListener.CLOSE);
      } else {
         LogMessages.LOGGER.info(Messages.MESSAGES.exceptionCaught(), e);
         ctx.close();
      }
   }

   @Override
   public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
      if (evt instanceof IdleStateEvent) {
         ctx.close();
      }
   }
}
