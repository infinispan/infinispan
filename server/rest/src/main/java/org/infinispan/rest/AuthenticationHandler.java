package org.infinispan.rest;

import static io.netty.handler.codec.http.HttpResponseStatus.UNAUTHORIZED;

import java.util.Objects;

import javax.security.auth.Subject;

import org.infinispan.rest.authentication.Authenticator;
import org.infinispan.rest.logging.Log;
import org.infinispan.util.logging.LogFactory;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class AuthenticationHandler extends BaseHttpRequestHandler {
   final static Log logger = LogFactory.getLog(AuthenticationHandler.class, Log.class);
   private final Authenticator authenticator;
   private Subject subject;
   private String authorization;

   public AuthenticationHandler(Authenticator authenticator) {
      this.authenticator = authenticator;
   }

   @Override
   protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request) {
      if (subject != null) {
         // Ensure that the authorization header, if needed, has not changed
         String authz = request.headers().get(HttpHeaderNames.AUTHORIZATION);
         if (Objects.equals(authz, authorization)) {
            ctx.fireChannelRead(request);
            return;
         } else {
            // Invalidate and force reauthentication
            subject = null;
            authorization = null;
         }
      }
      restAccessLoggingHandler.preLog(request);
      NettyRestRequest nettyRequest = new NettyRestRequest(request);
      authenticator.challenge(nettyRequest, ctx).whenComplete((authResponse, authThrowable) -> {
         boolean hasError = authThrowable != null;
         boolean authorized = authResponse.getStatus() != UNAUTHORIZED.code();
         if (!hasError && authorized) {
            subject = nettyRequest.getSubject();
            authorization = nettyRequest.getAuthorizationHeader();
            ctx.fireChannelRead(request);
         } else {
            try {
               if (hasError) {
                  handleError(ctx, request, authThrowable);
               } else {
                  sendResponse(ctx, request, ((NettyRestResponse) authResponse).getResponse());
               }
            } finally {
               request.release();
            }
         }
      });
   }

   public Subject getSubject() {
      return subject;
   }

   @Override
   protected Log getLogger() {
      return logger;
   }
}
