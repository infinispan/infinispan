package org.infinispan.client.hotrod.impl.transport.netty;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.security.Principal;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.security.Provider;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

import javax.net.ssl.SNIHostName;
import javax.net.ssl.SSLParameters;
import javax.security.auth.Subject;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslClientFactory;
import javax.security.sasl.SaslException;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.AuthenticationConfiguration;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.configuration.SslConfiguration;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.commons.util.SaslUtils;
import org.infinispan.commons.util.Util;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;

class ChannelInitializer extends io.netty.channel.ChannelInitializer<Channel> {
   private static final Log log = LogFactory.getLog(ChannelInitializer.class);

   private final Bootstrap bootstrap;
   private final SocketAddress unresolvedAddress;
   private final Configuration configuration;
   private final String hostNameToUse;
   private final SslContext sslContext;
   private final OperationDispatcher dispatcher;
   private final Consumer<ChannelPipeline> pipelineDecorator;

   private static final Provider[] SECURITY_PROVIDERS;

   static {
      // Register only the providers that matter to us
      List<Provider> providers = new ArrayList<>();
      for (String name : Arrays.asList(
            "org.wildfly.security.sasl.plain.WildFlyElytronSaslPlainProvider",
            "org.wildfly.security.sasl.digest.WildFlyElytronSaslDigestProvider",
            "org.wildfly.security.sasl.external.WildFlyElytronSaslExternalProvider",
            "org.wildfly.security.sasl.oauth2.WildFlyElytronSaslOAuth2Provider",
            "org.wildfly.security.sasl.scram.WildFlyElytronSaslScramProvider",
            "org.wildfly.security.sasl.gssapi.WildFlyElytronSaslGssapiProvider",
            "org.wildfly.security.sasl.gs2.WildFlyElytronSaslGs2Provider"
      )) {
         Provider provider = Util.getInstance(name, RemoteCacheManager.class.getClassLoader());
         providers.add(provider);
      }
      SECURITY_PROVIDERS = providers.toArray(new Provider[0]);
   }

   ChannelInitializer(Bootstrap bootstrap, SocketAddress unresolvedAddress, Configuration configuration,
                      String hostNameToUse, SslContext sslContext, OperationDispatcher dispatcher,
                      Consumer<ChannelPipeline> pipelineDecorator) {
      this.bootstrap = bootstrap;
      this.unresolvedAddress = unresolvedAddress;
      this.configuration = configuration;
      this.hostNameToUse = hostNameToUse;
      this.sslContext = sslContext;
      this.dispatcher = dispatcher;
      this.pipelineDecorator = pipelineDecorator;
   }

   ChannelFuture createChannel() {
      if (bootstrap.config().group().isShuttingDown()) {
         throw new IllegalStateException("Event loop is already shutdown!");
      }
      return bootstrap.clone().connect();
   }

   @Override
   protected void initChannel(Channel channel) throws Exception {
      if (log.isTraceEnabled()) {
         log.tracef("Created channel %s", channel);
      }
      if (configuration.security().ssl().enabled()) {
         initSsl(channel);
      }

      AuthenticationConfiguration authentication = configuration.security().authentication();
      if (authentication.enabled()) {
         initAuthentication(channel, authentication);
      } else {
         channel.pipeline().addLast(ActivationHandler.NAME, ActivationHandler.INSTANCE);
      }

      // TODO: make the reader and writer idle time configurable
      channel.pipeline().addLast("idleStateHandler", new IdleStateHandlerNoUnvoid(60, 30, 0));
      channel.pipeline().addLast("pingHandler", new PingHandler());
      channel.pipeline().addLast(HeaderDecoder.NAME, new HeaderDecoder(configuration, dispatcher));

      pipelineDecorator.accept(channel.pipeline());
   }

   private void initSsl(Channel channel) {
      SslConfiguration ssl = configuration.security().ssl();
      SslHandler sslHandler = sslContext.newHandler(channel.alloc(), ssl.sniHostName(), -1);
      String sniHostName;
      if (hostNameToUse != null) {
         sniHostName = hostNameToUse;
      } else if (ssl.sniHostName() != null) {
         sniHostName = ssl.sniHostName();
      } else {
         sniHostName = ((InetSocketAddress) unresolvedAddress).getHostString();
      }
      SSLParameters sslParameters = sslHandler.engine().getSSLParameters();
      sslParameters.setServerNames(Collections.singletonList(new SNIHostName(sniHostName)));
      if (ssl.hostnameValidation()) {
         sslParameters.setEndpointIdentificationAlgorithm("HTTPS");
      }
      sslHandler.engine().setSSLParameters(sslParameters);
      channel.pipeline().addFirst(sslHandler, SslHandshakeExceptionHandler.INSTANCE);
   }

   private void initAuthentication(Channel channel, AuthenticationConfiguration authentication) throws PrivilegedActionException, SaslException {
      SaslClient saslClient;
      SaslClientFactory scf = getSaslClientFactory(authentication);
      SslHandler sslHandler = channel.pipeline().get(SslHandler.class);
      Principal principal = sslHandler != null ? sslHandler.engine().getSession().getLocalPrincipal() : null;
      String authorizationId = principal != null ? principal.getName() : null;
      if (authentication.clientSubject() != null) {
         // We must use Subject.doAs() instead of Security.doAs()
         saslClient = Subject.doAs(authentication.clientSubject(), (PrivilegedExceptionAction<SaslClient>) () ->
               scf.createSaslClient(new String[]{authentication.saslMechanism()}, authorizationId, "hotrod",
                     authentication.serverName(), authentication.saslProperties(), authentication.callbackHandler())
         );
      } else {
         saslClient = scf.createSaslClient(new String[]{authentication.saslMechanism()}, authorizationId, "hotrod",
               authentication.serverName(), authentication.saslProperties(), authentication.callbackHandler());
      }

      channel.pipeline().addLast(AuthHandler.NAME, new AuthHandler(authentication, saslClient));
   }

   private SaslClientFactory getSaslClientFactory(AuthenticationConfiguration configuration) {
      if (log.isTraceEnabled()) {
         log.tracef("Attempting to load SaslClientFactory implementation with mech=%s, props=%s",
               configuration.saslMechanism(), configuration.saslProperties());
      }
      Collection<SaslClientFactory> clientFactories = SaslUtils.getSaslClientFactories(this.getClass().getClassLoader(), SECURITY_PROVIDERS, true);
      for (SaslClientFactory saslFactory : clientFactories) {
         try {
            String[] saslFactoryMechs = saslFactory.getMechanismNames(configuration.saslProperties());
            for (String supportedMech : saslFactoryMechs) {
               if (supportedMech.equals(configuration.saslMechanism())) {
                  if (log.isTraceEnabled()) {
                     log.tracef("Loaded SaslClientFactory: %s", saslFactory.getClass().getName());
                  }
                  return saslFactory;
               }

            }
         } catch (Throwable t) {
            // Catch any errors that can happen when calling to a Sasl mech
            log.tracef("Error while trying to obtain mechanism names supported by SaslClientFactory: %s", saslFactory.getClass().getName());
         }
      }
      throw new IllegalStateException("SaslClientFactory implementation not found");
   }
}
