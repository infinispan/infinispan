package org.infinispan.server.endpoint.subsystem.security;

import static org.wildfly.security.http.HttpConstants.SECURITY_IDENTITY;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;

import javax.security.auth.Subject;

import org.infinispan.rest.RestResponseException;
import org.infinispan.rest.RestServer;
import org.infinispan.rest.authentication.Authenticator;
import org.infinispan.rest.framework.RestRequest;
import org.infinispan.rest.framework.RestResponse;
import org.jboss.as.core.security.RealmRole;
import org.wildfly.security.auth.server.HttpAuthenticationFactory;
import org.wildfly.security.auth.server.SecurityIdentity;
import org.wildfly.security.http.HttpAuthenticationException;
import org.wildfly.security.http.HttpServerAuthenticationMechanism;

import io.netty.channel.ChannelHandlerContext;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class SecurityRealmRestAuthenticator implements Authenticator {
   private final HttpAuthenticationFactory factory;
   private final String mechanismName;
   private Executor executor;

   public SecurityRealmRestAuthenticator(HttpAuthenticationFactory factory, String mechanism) {
      this.factory = factory;
      this.mechanismName = mechanism;
   }

   @Override
   public CompletionStage<RestResponse> challenge(RestRequest request, ChannelHandlerContext ctx) {
      HttpServerRequestAdapter requestAdapter = new HttpServerRequestAdapter(request, ctx);
      return CompletableFuture.supplyAsync(() -> {
         try {
            HttpServerAuthenticationMechanism mechanism = factory.createMechanism(this.mechanismName);
            mechanism.evaluateRequest(requestAdapter);
            SecurityIdentity securityIdentity = (SecurityIdentity) mechanism.getNegotiatedProperty(SECURITY_IDENTITY);
            if (securityIdentity != null) {
               Subject subject = new Subject();
               subject.getPrincipals().add(securityIdentity.getPrincipal());
               securityIdentity.getRoles().forEach(r -> subject.getPrincipals().add(new RealmRole(r)));
               request.setSubject(subject);
            }
            return requestAdapter.getResponse();
         } catch (HttpAuthenticationException e) {
            throw new RestResponseException(e);
         }
      }, executor);
   }

   @Override
   public void init(RestServer restServer) {
      executor = restServer.getExecutor();
   }
}
