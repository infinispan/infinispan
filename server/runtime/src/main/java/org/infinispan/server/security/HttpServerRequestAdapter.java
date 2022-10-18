package org.infinispan.server.security;

import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.security.cert.Certificate;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;

import org.infinispan.rest.NettyRestResponse;
import org.infinispan.rest.framework.RestRequest;
import org.infinispan.rest.framework.RestResponse;
import org.wildfly.security.http.HttpAuthenticationException;
import org.wildfly.security.http.HttpScope;
import org.wildfly.security.http.HttpServerCookie;
import org.wildfly.security.http.HttpServerMechanismsResponder;
import org.wildfly.security.http.HttpServerRequest;
import org.wildfly.security.http.Scope;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.AttributeKey;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class HttpServerRequestAdapter implements HttpServerRequest {
   private final RestRequest request;
   private final ChannelHandlerContext ctx;
   private static final AttributeKey HTTP_SCOPE_ATTACHMENT_KEY = AttributeKey.newInstance(HttpScope.class.getSimpleName());
   NettyRestResponse.Builder responseBuilder;

   public HttpServerRequestAdapter(RestRequest request, ChannelHandlerContext ctx) {
      this.request = request;
      this.ctx = ctx;
      this.responseBuilder = new NettyRestResponse.Builder();
   }

   @Override
   public List<String> getRequestHeaderValues(String s) {
      return request.headers(s);
   }

   @Override
   public String getFirstRequestHeaderValue(String s) {
      return request.header(s);
   }

   @Override
   public SSLSession getSSLSession() {
      ChannelPipeline pipeline = ctx.pipeline();
      SslHandler sslHandler = (SslHandler) pipeline.get(SslHandler.class.getName());
      if (sslHandler == null && pipeline.channel().parent() != null) {
         sslHandler = (SslHandler) pipeline.channel().parent().pipeline().get(SslHandler.class.getName());
      }
      return sslHandler != null ? sslHandler.engine().getSession() : null;
   }

   @Override
   public Certificate[] getPeerCertificates() {
      SSLSession sslSession = getSSLSession();
      try {
         return sslSession != null ? sslSession.getPeerCertificates() : new Certificate[0];
      } catch (SSLPeerUnverifiedException e) {
         throw new RuntimeException(e);
      }
   }

   @Override
   public void noAuthenticationInProgress(HttpServerMechanismsResponder responder) {
      HttpServerResponseAdapter.adapt(responder, responseBuilder);
   }

   @Override
   public void authenticationInProgress(HttpServerMechanismsResponder responder) {
      HttpServerResponseAdapter.adapt(responder, responseBuilder);
   }

   @Override
   public void authenticationComplete(HttpServerMechanismsResponder responder) {
      HttpServerResponseAdapter.adapt(responder, responseBuilder);
   }

   @Override
   public void authenticationComplete(HttpServerMechanismsResponder responder, Runnable runnable) {
      HttpServerResponseAdapter.adapt(responder, responseBuilder);
   }

   @Override
   public void authenticationFailed(String s, HttpServerMechanismsResponder responder) {
      HttpServerResponseAdapter.adapt(responder, responseBuilder);
   }

   @Override
   public void badRequest(HttpAuthenticationException e, HttpServerMechanismsResponder responder) {
      HttpServerResponseAdapter.adapt(responder, responseBuilder);
   }

   @Override
   public String getRequestMethod() {
      return request.method().name();
   }

   @Override
   public URI getRequestURI() {
      return URI.create(request.uri());
   }

   @Override
   public String getRequestPath() {
      return request.path();
   }

   @Override
   public Map<String, List<String>> getParameters() {
      return request.parameters();
   }

   @Override
   public Set<String> getParameterNames() {
      return request.parameters().keySet();
   }

   @Override
   public List<String> getParameterValues(String s) {
      return request.parameters().get(s);
   }

   @Override
   public String getFirstParameterValue(String s) {
      return request.parameters().get(s).get(0);
   }

   @Override
   public List<HttpServerCookie> getCookies() {
      return Collections.emptyList();
   }

   @Override
   public InputStream getInputStream() {
      return null;
   }

   @Override
   public InetSocketAddress getSourceAddress() {
      return (InetSocketAddress) ctx.channel().remoteAddress();
   }

   @Override
   public boolean suspendRequest() {
      return false;
   }

   @Override
   public boolean resumeRequest() {
      return false;
   }

   @Override
   public HttpScope getScope(Scope scope) {
      switch (scope) {
         case CONNECTION:
            return getScope(ctx.channel());
         case GLOBAL:
            return null;
         case SSL_SESSION:
            return getScope(getSSLSession());
      }
      return null;
   }


   @Override
   public Collection<String> getScopeIds(Scope scope) {
      return null;
   }

   @Override
   public HttpScope getScope(Scope scope, String s) {
      return null;
   }

   public RestResponse getResponse() {
      return responseBuilder.build();
   }

   private HttpScope getScope(Channel channel) {
      HttpScope httpScope = (HttpScope) channel.attr(HTTP_SCOPE_ATTACHMENT_KEY).get();
      if (httpScope == null) {
         synchronized (channel) {
            httpScope = (HttpScope) channel.attr(HTTP_SCOPE_ATTACHMENT_KEY).get();
            if (httpScope == null) {
               final Map<String, Object> storageMap = new HashMap<>();
               httpScope = new HttpScope() {

                  @Override
                  public boolean exists() {
                     return true;
                  }

                  @Override
                  public boolean create() {
                     return false;
                  }

                  @Override
                  public boolean supportsAttachments() {
                     return true;
                  }

                  @Override
                  public void setAttachment(String key, Object value) {
                     if (value != null) {
                        storageMap.put(key, value);
                     } else {
                        storageMap.remove(key);
                     }
                  }

                  @Override
                  public Object getAttachment(String key) {
                     return storageMap.get(key);
                  }

               };
               channel.attr(HTTP_SCOPE_ATTACHMENT_KEY).set(httpScope);
            }
         }
      }

      return httpScope;
   }

   private HttpScope getScope(final SSLSession sslSession) {
      if (sslSession == null) {
         return null;
      }

      return new HttpScope() {

         @Override
         public boolean exists() {
            return true;
         }

         @Override
         public boolean create() {
            return false;
         }

         @Override
         public boolean supportsAttachments() {
            return true;
         }

         @Override
         public void setAttachment(String key, Object value) {
            sslSession.putValue(key, value);
         }

         @Override
         public Object getAttachment(String key) {
            return sslSession.getValue(key);
         }

      };
   }
}
