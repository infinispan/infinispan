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

import org.infinispan.rest.InfinispanErrorResponse;
import org.infinispan.rest.InfinispanRequest;
import org.infinispan.rest.InfinispanResponse;
import org.infinispan.rest.authentication.AuthenticationException;
import org.wildfly.security.http.HttpAuthenticationException;
import org.wildfly.security.http.HttpScope;
import org.wildfly.security.http.HttpServerCookie;
import org.wildfly.security.http.HttpServerMechanismsResponder;
import org.wildfly.security.http.HttpServerRequest;
import org.wildfly.security.http.Scope;

import io.netty.handler.ssl.SslHandler;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class HttpServerRequestAdapter implements HttpServerRequest {
   private final InfinispanRequest request;
   private InfinispanResponse response;

   public HttpServerRequestAdapter(InfinispanRequest request) {
      this.request = request;
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
      SslHandler sslHandler = (SslHandler) request.getRawContext().pipeline().get(SslHandler.class.getName());
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
      return request.method();
   }

   @Override
   public URI getRequestURI() {
      return URI.create(request.uri());
   }

   @Override
   public String getRequestPath() {
      return request.uri();
   }

   @Override
   public Map<String, List<String>> getParameters() {
      return request.getParameters();
   }

   @Override
   public Set<String> getParameterNames() {
      return request.getParameterNames();
   }

   @Override
   public List<String> getParameterValues(String s) {
      return request.getParameterValues(s);
   }

   @Override
   public String getFirstParameterValue(String s) {
      return request.getParameterValue(s);
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
      return (InetSocketAddress)request.getRawContext().channel().remoteAddress();
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

   public void validateResponse() {
      // Converts the Elytron response into an AuthenticationException
      if (response != null && response instanceof InfinispanErrorResponse) {
         throw new AuthenticationException(response.authenticateHeader());
      }
   }
}
