package org.infinispan.client.hotrod.impl.transport.netty;

import java.net.SocketAddress;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import javax.net.ssl.SNIHostName;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslClientFactory;
import javax.security.sasl.SaslException;

import org.infinispan.client.hotrod.configuration.AuthenticationConfiguration;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.configuration.SslConfiguration;
import org.infinispan.client.hotrod.impl.operations.OperationsFactory;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.util.SaslUtils;
import org.infinispan.commons.util.SslContextFactory;

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
   private static final CallbackHandler NOOP_HANDLER = callbacks -> {};
   private static Log log = LogFactory.getLog(ChannelInitializer.class);
   private static boolean trace = log.isTraceEnabled();

   private final Bootstrap bootstrap;
   private final SocketAddress unresolvedAddress;
   private final OperationsFactory operationsFactory;
   private final Configuration configuration;
   private ChannelPool channelPool;
   private volatile boolean isFirstPing = true;

   ChannelInitializer(Bootstrap bootstrap, SocketAddress unresolvedAddress, OperationsFactory operationsFactory, Configuration configuration) {
      this.bootstrap = bootstrap;
      this.unresolvedAddress = unresolvedAddress;
      this.operationsFactory = operationsFactory;
      this.configuration = configuration;
   }

   CompletableFuture<Channel> createChannel() {
      ChannelFuture connect = bootstrap.clone().connect();
      ActivationFuture activationFuture = new ActivationFuture();
      connect.addListener(activationFuture);
      return activationFuture;
   }

   @Override
   protected void initChannel(Channel channel) throws Exception {
      if (trace) {
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
         channel.pipeline().addLast(
               new IdleStateHandler(0, 0, configuration.connectionPool().minEvictableIdleTime(), TimeUnit.MILLISECONDS),
               new IdleStateHandlerProvider(configuration.connectionPool().minIdle(), channelPool));
      }
      ChannelRecord channelRecord = new ChannelRecord(unresolvedAddress, channelPool);
      channel.attr(ChannelRecord.KEY).set(channelRecord);
      if (isFirstPing) {
         isFirstPing = false;
         channel.pipeline().addLast(new InitialPingHandler(operationsFactory.newPingOperation(false)));
      } else {
         channel.pipeline().addLast(ActivationHandler.INSTANCE);
      }
   }

   private void initSsl(Channel channel) {
      SslConfiguration ssl = configuration.security().ssl();
      SslContext nettySslContext;
      SSLContext jdkSslContext = ssl.sslContext();
      if (jdkSslContext == null) {
         SslContextBuilder builder = SslContextBuilder.forClient();
         try {
            if (ssl.keyStoreFileName() != null) {
               builder.keyManager(SslContextFactory.getKeyManagerFactory(
                     ssl.keyStoreFileName(),
                     ssl.keyStoreType(),
                     ssl.keyStorePassword(),
                     ssl.keyStoreCertificatePassword(),
                     ssl.keyAlias(),
                     configuration.classLoader()));
            }
            if (ssl.trustStoreFileName() != null) {
               builder.trustManager(SslContextFactory.getTrustManagerFactory(
                     ssl.trustStoreFileName(),
                     ssl.trustStoreType(),
                     ssl.trustStorePassword(),
                     configuration.classLoader()));
            }
            if (ssl.protocol() != null) {
               builder.protocols(ssl.protocol());
            }
            nettySslContext = builder.build();
         } catch (Exception e) {
            throw new CacheConfigurationException(e);
         }
      } else {
         nettySslContext = new JdkSslContext(jdkSslContext, true, ClientAuth.NONE);
      }
      SslHandler sslHandler = nettySslContext.newHandler(channel.alloc(), ssl.sniHostName(), -1);
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
      if (authentication.clientSubject() != null) {
         saslClient = Subject.doAs(authentication.clientSubject(), (PrivilegedExceptionAction<SaslClient>) () -> {
            CallbackHandler callbackHandler = authentication.callbackHandler();
            if (callbackHandler == null) {
               callbackHandler = NOOP_HANDLER;
            }
            return scf.createSaslClient(new String[]{authentication.saslMechanism()}, null, "hotrod",
                  authentication.serverName(), authentication.saslProperties(), callbackHandler);
         });
      } else {
         saslClient = scf.createSaslClient(new String[]{authentication.saslMechanism()}, null, "hotrod",
               authentication.serverName(), authentication.saslProperties(), authentication.callbackHandler());
      }

      channel.pipeline().addLast(new AuthHandler(authentication, saslClient, operationsFactory));
   }

   private SaslClientFactory getSaslClientFactory(AuthenticationConfiguration configuration) {
      if (trace) {
         log.tracef("Attempting to load SaslClientFactory implementation with mech=%s, props=%s",
               configuration.saslMechanism(), configuration.saslProperties());
      }
      Iterator<SaslClientFactory> clientFactories = SaslUtils.getSaslClientFactories(this.getClass().getClassLoader(), true);
      while (clientFactories.hasNext()) {
         SaslClientFactory saslFactory = clientFactories.next();
         String[] saslFactoryMechs = saslFactory.getMechanismNames(configuration.saslProperties());
         for (String supportedMech : saslFactoryMechs) {
            if (supportedMech.equals(configuration.saslMechanism())) {
               if (trace) {
                  log.tracef("Loaded SaslClientFactory: %s", saslFactory.getClass().getName());
               }
               return saslFactory;
            }
         }
      }
      throw new IllegalStateException("SaslClientFactory implementation now found");
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
