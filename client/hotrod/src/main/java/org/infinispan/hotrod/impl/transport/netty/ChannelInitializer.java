package org.infinispan.hotrod.impl.transport.netty;

import java.io.File;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import javax.net.ssl.SNIHostName;
import javax.net.ssl.SSLParameters;
import javax.security.auth.Subject;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslClientFactory;
import javax.security.sasl.SaslException;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.util.SaslUtils;
import org.infinispan.commons.util.SslContextFactory;
import org.infinispan.commons.util.Util;
import org.infinispan.hotrod.HotRod;
import org.infinispan.hotrod.configuration.AuthenticationConfiguration;
import org.infinispan.hotrod.configuration.HotRodConfiguration;
import org.infinispan.hotrod.configuration.SslConfiguration;
import org.infinispan.hotrod.impl.logging.Log;
import org.infinispan.hotrod.impl.logging.LogFactory;
import org.infinispan.hotrod.impl.operations.CacheOperationsFactory;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.JdkSslContext;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.timeout.IdleStateHandler;

class ChannelInitializer extends io.netty.channel.ChannelInitializer<Channel> {
   private static final Log log = LogFactory.getLog(ChannelInitializer.class);

   private final Bootstrap bootstrap;
   private final SocketAddress unresolvedAddress;
   private final CacheOperationsFactory cacheOperationsFactory;
   private final HotRodConfiguration configuration;
   private final ChannelFactory channelFactory;
   private ChannelPool channelPool;
   private volatile boolean isFirstPing = true;

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
         Provider provider = Util.getInstance(name, HotRod.class.getClassLoader());
         providers.add(provider);
      }
      SECURITY_PROVIDERS = providers.toArray(new Provider[0]);
   }

   ChannelInitializer(Bootstrap bootstrap, SocketAddress unresolvedAddress, CacheOperationsFactory cacheOperationsFactory, HotRodConfiguration configuration, ChannelFactory channelFactory) {
      this.bootstrap = bootstrap;
      this.unresolvedAddress = unresolvedAddress;
      this.cacheOperationsFactory = cacheOperationsFactory;
      this.configuration = configuration;
      this.channelFactory = channelFactory;
   }

   CompletableFuture<Channel> createChannel() {
      ChannelFuture connect = bootstrap.clone().connect();
      ActivationFuture activationFuture = new ActivationFuture();
      connect.addListener(activationFuture);
      return activationFuture;
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
      }

      if (configuration.connectionPool().minEvictableIdleTime() > 0) {
         channel.pipeline().addLast("idle-state-handler",
               new IdleStateHandler(0, 0, configuration.connectionPool().minEvictableIdleTime(), TimeUnit.MILLISECONDS));
      }
      ChannelRecord channelRecord = new ChannelRecord(unresolvedAddress, channelPool);
      channel.attr(ChannelRecord.KEY).set(channelRecord);
      if (isFirstPing) {
         isFirstPing = false;
         channel.pipeline().addLast(InitialPingHandler.NAME, new InitialPingHandler(cacheOperationsFactory.newPingOperation(false)));
      } else {
         channel.pipeline().addLast(ActivationHandler.NAME, ActivationHandler.INSTANCE);
      }
      channel.pipeline().addLast(HeaderDecoder.NAME, new HeaderDecoder(cacheOperationsFactory.getDefaultContext()));
      if (configuration.connectionPool().minEvictableIdleTime() > 0) {
         // This handler needs to be the last so that HeaderDecoder has the chance to cancel the idle event
         channel.pipeline().addLast(IdleStateHandlerProvider.NAME,
               new IdleStateHandlerProvider(configuration.connectionPool().minIdle(), channelPool));
      }
   }

   private void initSsl(Channel channel) {
      SslConfiguration ssl = configuration.security().ssl();
      SslContext sslContext;
      if (ssl.sslContext() == null) {
         SslContextBuilder builder = SslContextBuilder.forClient();
         try {
            if (ssl.keyStoreFileName() != null) {
               builder.keyManager(new SslContextFactory()
                     .keyStoreFileName(ssl.keyStoreFileName())
                     .keyStoreType(ssl.keyStoreType())
                     .keyStorePassword(ssl.keyStorePassword())
                     .keyAlias(ssl.keyAlias())
                     .classLoader(HotRod.class.getClassLoader())
                     .provider(ssl.provider())
                     .getKeyManagerFactory());
            }
            if (ssl.trustStoreFileName() != null) {
               if ("pem".equalsIgnoreCase(ssl.trustStoreType())) {
                  builder.trustManager(new File(ssl.trustStoreFileName()));
               } else {
                  builder.trustManager(new SslContextFactory()
                        .trustStoreFileName(ssl.trustStoreFileName())
                        .trustStoreType(ssl.trustStoreType())
                        .trustStorePassword(ssl.trustStorePassword())
                        .provider(ssl.provider())
                        .classLoader(HotRod.class.getClassLoader())
                        .getTrustManagerFactory());
               }
            }
            if (ssl.protocol() != null) {
               builder.protocols(ssl.protocol());
            }
            if (ssl.ciphers() != null) {
               builder.ciphers(Arrays.asList(ssl.ciphers()));
            }
            if (ssl.provider() != null) {
               Provider provider = SslContextFactory.findProvider(ssl.provider(), SslContext.class.getSimpleName(), "TLS");
               builder.sslContextProvider(provider);
            }
            sslContext = builder.build();
         } catch (Exception e) {
            throw new CacheConfigurationException(e);
         }
      } else {
         sslContext = new JdkSslContext(ssl.sslContext(), true, ClientAuth.NONE);
      }
      SslHandler sslHandler = sslContext.newHandler(channel.alloc(), ssl.sniHostName(), -1);
      if (ssl.sniHostName() != null) {
         SSLParameters sslParameters = sslHandler.engine().getSSLParameters();
         sslParameters.setServerNames(Collections.singletonList(new SNIHostName(ssl.sniHostName())));
         sslHandler.engine().setSSLParameters(sslParameters);
      }
      channel.pipeline().addFirst(sslHandler,
            SslHandshakeExceptionHandler.INSTANCE);
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

      channel.pipeline().addLast(AuthHandler.NAME, new AuthHandler(authentication, saslClient, cacheOperationsFactory));
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

   void setChannelPool(ChannelPool channelPool) {
      this.channelPool = channelPool;
   }

   private static class ActivationFuture extends CompletableFuture<Channel> implements ChannelFutureListener, BiConsumer<Channel, Throwable> {
      @Override
      public void operationComplete(ChannelFuture future) throws Exception {
         if (future.isSuccess()) {
            Channel channel = future.channel();
            ChannelRecord.of(channel).whenComplete(this);
         } else {
            completeExceptionally(future.cause());
         }
      }

      @Override
      public void accept(Channel channel, Throwable throwable) {
         if (throwable != null) {
            completeExceptionally(throwable);
         } else {
            complete(channel);
         }
      }
   }
}
