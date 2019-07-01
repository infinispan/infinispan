package org.infinispan.server.endpoint.subsystem.security;

import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.security.cert.Certificate;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;

import org.infinispan.rest.framework.RestRequest;
import org.infinispan.rest.framework.RestResponse;
import org.wildfly.security.http.HttpAuthenticationException;
import org.wildfly.security.http.HttpScope;
import org.wildfly.security.http.HttpServerCookie;
import org.wildfly.security.http.HttpServerMechanismsResponder;
import org.wildfly.security.http.HttpServerRequest;
import org.wildfly.security.http.Scope;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.ssl.SslHandler;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class HttpServerRequestAdapter implements HttpServerRequest {
   private final RestRequest request;
   private RestResponse response;
   private final ChannelHandlerContext ctx;

   public HttpServerRequestAdapter(RestRequest request, ChannelHandlerContext ctx) {
      this.request = request;
      this.ctx = ctx;
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
      SslHandler sslHandler = (SslHandler) ctx.pipeline().get(SslHandler.class.getName());
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
      response = HttpServerResponseAdapter.getResponse(responder);
   }

   @Override
   public void authenticationInProgress(HttpServerMechanismsResponder responder) {
      response = HttpServerResponseAdapter.getResponse(responder);
   }

   @Override
   public void authenticationComplete(HttpServerMechanismsResponder responder) {
      response = HttpServerResponseAdapter.getResponse(responder);
   }

   @Override
   public void authenticationComplete(HttpServerMechanismsResponder responder, Runnable runnable) {
      response = HttpServerResponseAdapter.getResponse(responder);
   }

   @Override
   public void authenticationFailed(String s, HttpServerMechanismsResponder responder) {
      response = HttpServerResponseAdapter.getResponse(responder);
   }

   @Override
   public void badRequest(HttpAuthenticationException e, HttpServerMechanismsResponder responder) {
      response = HttpServerResponseAdapter.getResponse(responder);
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
      return (InetSocketAddress)ctx.channel().remoteAddress();
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
      return response;
   }
}
