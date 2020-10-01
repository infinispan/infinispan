package org.infinispan.server.security;

import static org.wildfly.security.http.HttpConstants.SECURITY_IDENTITY;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;

import javax.security.auth.Subject;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.rest.NettyRestResponse;
import org.infinispan.rest.RestServer;
import org.infinispan.rest.authentication.Authenticator;
import org.infinispan.rest.configuration.RestServerConfiguration;
import org.infinispan.rest.framework.RestRequest;
import org.infinispan.rest.framework.RestResponse;
import org.wildfly.security.auth.server.MechanismConfiguration;
import org.wildfly.security.auth.server.MechanismConfigurationSelector;
import org.wildfly.security.auth.server.MechanismRealmConfiguration;
import org.wildfly.security.auth.server.SecurityIdentity;
import org.wildfly.security.auth.server.http.HttpAuthenticationFactory;
import org.wildfly.security.http.HttpAuthenticationException;
import org.wildfly.security.http.HttpServerAuthenticationMechanism;
import org.wildfly.security.http.HttpServerAuthenticationMechanismFactory;
import org.wildfly.security.http.digest.DigestMechanismFactory;
import org.wildfly.security.http.util.SecurityProviderServerMechanismFactory;
import org.wildfly.security.http.util.SetMechanismInformationMechanismFactory;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.HttpResponseStatus;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class ElytronHTTPAuthenticator implements Authenticator {
   private final HttpAuthenticationFactory factory;
   private final ServerSecurityRealm serverSecurityRealm;
   private Executor executor;
   private RestServerConfiguration configuration;

   public ElytronHTTPAuthenticator(String name, ServerSecurityRealm realm, String serverPrincipal) {
      this.serverSecurityRealm = realm;
      HttpAuthenticationFactory.Builder httpBuilder = HttpAuthenticationFactory.builder();
      httpBuilder.setSecurityDomain(realm.getSecurityDomain());

      HttpServerAuthenticationMechanismFactory httpServerFactory = new SecurityProviderServerMechanismFactory();
      httpServerFactory = new SetMechanismInformationMechanismFactory(httpServerFactory);
      httpBuilder.setFactory(httpServerFactory);

      MechanismConfiguration.Builder mechConfigurationBuilder = MechanismConfiguration.builder();
      realm.applyServerCredentials(mechConfigurationBuilder, serverPrincipal);
      final MechanismRealmConfiguration.Builder mechRealmBuilder = MechanismRealmConfiguration.builder();
      mechRealmBuilder.setRealmName(name);
      mechConfigurationBuilder.addMechanismRealm(mechRealmBuilder.build());
      httpBuilder.setMechanismConfigurationSelector(MechanismConfigurationSelector.constantSelector(mechConfigurationBuilder.build()));

      factory = httpBuilder.build();
   }

   @Override
   public CompletionStage<RestResponse> challenge(RestRequest request, ChannelHandlerContext ctx) {
      HttpServerRequestAdapter requestAdapter = new HttpServerRequestAdapter(request, ctx);
      return CompletableFuture.supplyAsync(() -> {
         try {
            String authorizationHeader = request.getAuthorizationHeader();
            if (authorizationHeader == null) {
               for (String name : configuration.authentication().mechanisms()) {
                  HttpServerAuthenticationMechanism mechanism = factory.createMechanism(name);
                  mechanism.evaluateRequest(requestAdapter);
               }
               return requestAdapter.getResponse();
            } else {
               String mechName = authorizationHeader.substring(0, authorizationHeader.indexOf(' ')).toUpperCase();
               if ("BEARER".equals(mechName)) {
                  mechName = "BEARER_TOKEN";
               } else if ("NEGOTIATE".equals(mechName)) {
                  mechName = "SPNEGO";
               }
               HttpServerAuthenticationMechanism mechanism = factory.createMechanism(mechName);
               mechanism.evaluateRequest(requestAdapter);
               SecurityIdentity securityIdentity = (SecurityIdentity) mechanism.getNegotiatedProperty(SECURITY_IDENTITY);
               if (securityIdentity != null) {
                  Subject subject = new Subject();
                  subject.getPrincipals().add(securityIdentity.getPrincipal());
                  securityIdentity.getRoles().forEach(r -> subject.getPrincipals().add(new RolePrincipal(r)));
                  request.setSubject(subject);
               }
               return requestAdapter.getResponse();
            }
         } catch (HttpAuthenticationException e) {
            return new NettyRestResponse.Builder().status(HttpResponseStatus.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build();
         }
      }, executor);
   }

   public void init(RestServer restServer) {
      this.configuration = restServer.getConfiguration();
      this.executor = restServer.getExecutor();
      for (String name : configuration.authentication().mechanisms()) {
         try {
            factory.createMechanism(name);
         } catch (HttpAuthenticationException e) {
            throw new CacheConfigurationException("Could not create HTTP authentication mechanism " + name);
         }
      }
   }

   @Override
   public boolean isReadyForHttpChallenge() {
      return serverSecurityRealm.isReadyForHttpChallenge();
   }

   @Override
   public void close() {
      // Hack to shutdown the nonce executor
      if (!Boolean.getBoolean("infinispan.security.elytron.skipnonceshutdown")) {
         new DigestMechanismFactory().shutdown();
      }
      factory.shutdownAuthenticationMechanismFactory();
   }
}
