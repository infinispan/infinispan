package org.infinispan.rest.authentication.impl;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import javax.security.auth.Subject;

import org.infinispan.rest.NettyRestResponse;
import org.infinispan.rest.authentication.RestAuthenticator;
import org.infinispan.rest.framework.RestRequest;
import org.infinispan.rest.framework.RestResponse;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.ssl.SslHandler;

/**
 * Client Certificate authentication mechanism.
 *
 * @author Tristan Tarrant
 * @author Sebastian Łaskawiec
 */
public class ClientCertAuthenticator implements RestAuthenticator {

   public ClientCertAuthenticator() {
   }

   @Override
   public CompletionStage<RestResponse> challenge(RestRequest request, ChannelHandlerContext ctx) {
      try {
         SslHandler sslHandler = ctx.pipeline().get(SslHandler.class);
         SSLSession session = sslHandler.engine().getSession();
         Subject subject = new Subject();
         subject.getPrincipals().add(session.getPeerPrincipal());
         request.setSubject(subject);
         return CompletableFuture.completedFuture(new NettyRestResponse.Builder().build());
      } catch (SSLPeerUnverifiedException e) {
         // Ignore any SSLPeerUnverifiedExceptions
      }
      return CompletableFuture.completedFuture(new NettyRestResponse.Builder().status(HttpResponseStatus.UNAUTHORIZED).build());
   }
}
