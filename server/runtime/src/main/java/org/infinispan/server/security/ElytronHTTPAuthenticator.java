package org.infinispan.server.security;

import static org.wildfly.security.http.HttpConstants.SECURITY_IDENTITY;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;

import javax.security.auth.Subject;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.rest.RestServer;
import org.infinispan.rest.authentication.Authenticator;
import org.infinispan.rest.configuration.RestServerConfiguration;
import org.infinispan.rest.framework.RestRequest;
import org.infinispan.rest.framework.RestResponse;
import org.infinispan.server.Server;
import org.infinispan.server.configuration.ServerConfiguration;
import org.wildfly.security.auth.server.MechanismConfiguration;
import org.wildfly.security.auth.server.MechanismConfigurationSelector;
import org.wildfly.security.auth.server.MechanismRealmConfiguration;
import org.wildfly.security.auth.server.SecurityIdentity;
import org.wildfly.security.auth.server.http.HttpAuthenticationFactory;
import org.wildfly.security.http.HttpAuthenticationException;
import org.wildfly.security.http.HttpServerAuthenticationMechanism;
import org.wildfly.security.http.HttpServerAuthenticationMechanismFactory;
import org.wildfly.security.http.digest.DigestMechanismFactory;
import org.wildfly.security.http.util.FilterServerMechanismFactory;
import org.wildfly.security.http.util.SecurityProviderServerMechanismFactory;
import org.wildfly.security.http.util.SetMechanismInformationMechanismFactory;

import io.netty.channel.ChannelHandlerContext;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class ElytronHTTPAuthenticator implements Authenticator {
   private final String name;
   private final String serverPrincipal;
   private final Collection<String> mechanisms;
   private HttpAuthenticationFactory factory;
   private ServerSecurityRealm serverSecurityRealm;
   private Executor executor;
   private RestServerConfiguration configuration;

   public ElytronHTTPAuthenticator(String name, String serverPrincipal, Collection<String> mechanisms) {
      this.name = name;
      this.serverPrincipal = serverPrincipal;
      this.mechanisms = mechanisms;
   }

   public static void init(RestServerConfiguration configuration, ServerConfiguration serverConfiguration) {
      ElytronHTTPAuthenticator authenticator = (ElytronHTTPAuthenticator) configuration.authentication().authenticator();
      if (authenticator != null) {
         authenticator.init(serverConfiguration);
      }
   }

   public void init(ServerConfiguration serverConfiguration) {
      this.serverSecurityRealm = serverConfiguration.security().realms().getRealm(name).serverSecurityRealm();
      HttpAuthenticationFactory.Builder httpBuilder = HttpAuthenticationFactory.builder();
      httpBuilder.setSecurityDomain(serverSecurityRealm.getSecurityDomain());

      HttpServerAuthenticationMechanismFactory httpServerFactory = new SecurityProviderServerMechanismFactory();
      httpServerFactory = new SetMechanismInformationMechanismFactory(new FilterServerMechanismFactory(httpServerFactory, true, mechanisms));
      httpBuilder.setFactory(httpServerFactory);

      MechanismConfiguration.Builder mechConfigurationBuilder = MechanismConfiguration.builder();
      serverSecurityRealm.applyServerCredentials(mechConfigurationBuilder, serverPrincipal);
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
                  extractSubject(request, mechanism);
               }
            } else {
               String mechName = authorizationHeader.substring(0, authorizationHeader.indexOf(' ')).toUpperCase();
               if ("BEARER".equals(mechName)) {
                  mechName = "BEARER_TOKEN";
               } else if ("NEGOTIATE".equals(mechName)) {
                  mechName = "SPNEGO";
               }
               HttpServerAuthenticationMechanism mechanism = factory.createMechanism(mechName);
               if (mechanism == null) {
                  throw Server.log.unsupportedMechanism(mechName);
               }
               mechanism.evaluateRequest(requestAdapter);
               extractSubject(request, mechanism);
            }
            return requestAdapter.getResponse();
         } catch (Exception e) {
            throw e instanceof RuntimeException ? (RuntimeException) e : new RuntimeException(e);
         }
      }, executor);
   }

   private void extractSubject(RestRequest request, HttpServerAuthenticationMechanism mechanism) {
      SecurityIdentity securityIdentity = (SecurityIdentity) mechanism.getNegotiatedProperty(SECURITY_IDENTITY);
      if (securityIdentity != null) {
         Subject subject = new Subject();
         subject.getPrincipals().add(securityIdentity.getPrincipal());
         securityIdentity.getRoles().forEach(r -> subject.getPrincipals().add(new RolePrincipal(r)));
         request.setSubject(subject);
      }
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
