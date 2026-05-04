package org.infinispan.server.security;

import static org.wildfly.security.http.HttpConstants.SECURITY_IDENTITY;

import java.security.Provider;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import javax.security.auth.Subject;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.rest.RestServer;
import org.infinispan.rest.authentication.RestAuthenticator;
import org.infinispan.rest.configuration.RestServerConfiguration;
import org.infinispan.rest.framework.RestRequest;
import org.infinispan.rest.framework.RestResponse;
import org.infinispan.security.GroupPrincipal;
import org.infinispan.server.Server;
import org.infinispan.server.configuration.ServerConfiguration;
import org.infinispan.server.security.http.localuser.WildFlyElytronHttpLocalUserProvider;
import org.infinispan.util.concurrent.BlockingManager;
import org.wildfly.security.auth.server.MechanismConfiguration;
import org.wildfly.security.auth.server.MechanismConfigurationSelector;
import org.wildfly.security.auth.server.MechanismRealmConfiguration;
import org.wildfly.security.auth.server.SecurityIdentity;
import org.wildfly.security.auth.server.http.HttpAuthenticationFactory;
import org.wildfly.security.http.HttpAuthenticationException;
import org.wildfly.security.http.HttpServerAuthenticationMechanism;
import org.wildfly.security.http.HttpServerAuthenticationMechanismFactory;
import org.wildfly.security.http.basic.WildFlyElytronHttpBasicProvider;
import org.wildfly.security.http.bearer.WildFlyElytronHttpBearerProvider;
import org.wildfly.security.http.cert.WildFlyElytronHttpClientCertProvider;
import org.wildfly.security.http.digest.DigestMechanismFactory;
import org.wildfly.security.http.digest.WildFlyElytronHttpDigestProvider;
import org.wildfly.security.http.spnego.WildFlyElytronHttpSpnegoProvider;
import org.wildfly.security.http.util.FilterServerMechanismFactory;
import org.wildfly.security.http.util.SecurityProviderServerMechanismFactory;
import org.wildfly.security.http.util.SetMechanismInformationMechanismFactory;

import io.netty.channel.ChannelHandlerContext;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class ElytronHTTPAuthenticator implements RestAuthenticator {
   private final String name;
   private final String serverPrincipal;
   private final Collection<String> mechanisms;
   private final Provider[] providers;
   private final Map<String, String> challengePrefixMechanisms;
   private HttpAuthenticationFactory factory;
   private ServerSecurityRealm serverSecurityRealm;
   private BlockingManager blockingManager;
   private RestServerConfiguration configuration;


   public ElytronHTTPAuthenticator(String name, String serverPrincipal, Collection<String> mechanisms) {
      this.name = name;
      this.serverPrincipal = serverPrincipal;
      this.mechanisms = mechanisms;
      this.providers = new Provider[]{
            WildFlyElytronHttpBasicProvider.getInstance(),
            WildFlyElytronHttpBearerProvider.getInstance(),
            WildFlyElytronHttpDigestProvider.getInstance(),
            WildFlyElytronHttpClientCertProvider.getInstance(),
            WildFlyElytronHttpSpnegoProvider.getInstance(),
            WildFlyElytronHttpLocalUserProvider.getInstance(),
      };
      Map<String, String> prefixes = new HashMap<>();
      for (String m : mechanisms) {
         switch (m) {
            case "BASIC":
               prefixes.put("BASIC", "BASIC");
               break;
            case "SPNEGO":
               prefixes.put("NEGOTIATE", "SPNEGO");
               break;
            case "BEARER_TOKEN":
               prefixes.put("BEARER", "BEARER_TOKEN");
               break;
            case "DIGEST":
               prefixes.put("DIGEST", "DIGEST");
               break;
            case "DIGEST-SHA-256":
               prefixes.put("DIGEST", "DIGEST-SHA-256");
               break;
            case "DIGEST-SHA-512-256":
               prefixes.put("DIGEST", "DIGEST-SHA-512-256");
               break;
            case "LOCALUSER":
               prefixes.put("LOCALUSER", "LOCALUSER");
               break;
            default:
               prefixes.put(m, m);
         }
      }
      challengePrefixMechanisms = Map.copyOf(prefixes);
   }

   public static void init(RestServerConfiguration configuration, ServerConfiguration serverConfiguration) {
      ElytronHTTPAuthenticator authenticator = (ElytronHTTPAuthenticator) configuration.authentication().authenticator();
      if (authenticator != null) {
         authenticator.init(serverConfiguration);
      }
   }

   public Provider[] getProviders() {
      return providers;
   }

   public void setFactory(HttpAuthenticationFactory factory) {
      this.factory = factory;
   }

   public void init(ServerConfiguration serverConfiguration) {
      this.serverSecurityRealm = serverConfiguration.security().realms().getRealm(name).serverSecurityRealm();
      HttpAuthenticationFactory.Builder httpBuilder = HttpAuthenticationFactory.builder();
      httpBuilder.setSecurityDomain(serverSecurityRealm.getSecurityDomain());

      HttpServerAuthenticationMechanismFactory httpServerFactory = new SecurityProviderServerMechanismFactory(providers);
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
      return blockingManager.supplyBlocking(() -> {
         try {
            String authorizationHeader = request.getAuthorizationHeader();
            if (authorizationHeader == null) {
               challengeClient(request, requestAdapter);
            } else {
               int spaceIdx = authorizationHeader.indexOf(' ');
               String headerMechName = (spaceIdx >= 0 ? authorizationHeader.substring(0, spaceIdx) : authorizationHeader).toUpperCase();
               String mechName = challengePrefixMechanisms.get(headerMechName);
               if (mechName == null) {
                  // Unknown mechanism prefix: respond with available challenges
                  // instead of failing, so the client can retry with a supported mechanism
                  challengeClient(request, requestAdapter);
               } else {
                  HttpServerAuthenticationMechanism mechanism = factory.createMechanism(mechName);
                  if (mechanism == null) {
                     throw Server.log.unsupportedMechanism(headerMechName);
                  }
                  mechanism.evaluateRequest(requestAdapter);
                  extractSubject(request, mechanism);
               }
            }
            return requestAdapter.getResponse();
         } catch (Exception e) {
            throw e instanceof RuntimeException ? (RuntimeException) e : new RuntimeException(e);
         }
      }, "auth");
   }

   private void challengeClient(RestRequest request, HttpServerRequestAdapter requestAdapter) throws HttpAuthenticationException {
      for (String name : configuration.authentication().mechanisms()) {
         HttpServerAuthenticationMechanism mechanism = factory.createMechanism(name);
         mechanism.evaluateRequest(requestAdapter);
         extractSubject(request, mechanism);
      }
   }

   private void extractSubject(RestRequest request, HttpServerAuthenticationMechanism mechanism) {
      SecurityIdentity securityIdentity = (SecurityIdentity) mechanism.getNegotiatedProperty(SECURITY_IDENTITY);
      if (securityIdentity != null) {
         Subject subject = new Subject();
         subject.getPrincipals().add(securityIdentity.getPrincipal());
         securityIdentity.getRoles().forEach(r -> subject.getPrincipals().add(new GroupPrincipal(r)));
         request.setSubject(subject);
      }
   }

   public void init(RestServer restServer) {
      this.configuration = restServer.getConfiguration();
      this.blockingManager = restServer.getBlockingManager();
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
      if (Boolean.parseBoolean(System.getProperty(Server.INFINISPAN_ELYTRON_NONCE_SHUTDOWN, "true"))) {
         new DigestMechanismFactory().shutdown();
      }
      factory.shutdownAuthenticationMechanismFactory();
   }
}
