package org.infinispan.client.hotrod.impl.transport.netty;

import java.net.SocketAddress;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslClientFactory;
import javax.security.sasl.SaslException;

import org.infinispan.client.hotrod.configuration.AuthenticationConfiguration;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.operations.OperationsFactory;
import org.infinispan.client.hotrod.impl.transport.netty.singleport.SslHandlerFactory;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.commons.util.SaslUtils;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
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
   private final ChannelFactory channelFactory;
   private final SslHandlerFactory sslHandlerFactory;
   private ChannelPool channelPool;
   private volatile boolean isFirstPing = true;

   ChannelInitializer(Bootstrap bootstrap, SocketAddress unresolvedAddress, OperationsFactory operationsFactory, Configuration configuration, ChannelFactory channelFactory, SslHandlerFactory sslHandlerFactory) {
      this.bootstrap = bootstrap;
      this.unresolvedAddress = unresolvedAddress;
      this.operationsFactory = operationsFactory;
      this.configuration = configuration;
      this.channelFactory = channelFactory;
      this.sslHandlerFactory = sslHandlerFactory;
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

      if (sslHandlerFactory != null) {
         SslHandler sslHandler = sslHandlerFactory.getSslHandler(channel.alloc());
         channel.pipeline().addFirst(sslHandler, SslHandshakeExceptionHandler.INSTANCE);
      }

      AuthenticationConfiguration authentication = configuration.security().authentication();
      if (authentication.enabled()) {
         initAuthentication(channel, authentication);
      }

      if (configuration.connectionPool().minEvictableIdleTime() > 0) {
         channel.pipeline().addLast("idle-state-handler",
               new IdleStateHandler(0, 0, configuration.connectionPool().minEvictableIdleTime(), TimeUnit.MILLISECONDS));
      }
      if (isFirstPing) {
         isFirstPing = false;
         channel.pipeline().addLast(InitialPingHandler.NAME, new InitialPingHandler(operationsFactory.newPingOperation(false)));
      } else {
         channel.pipeline().addLast(ActivationHandler.NAME, ActivationHandler.INSTANCE);
      }
      channel.pipeline().addLast(HeaderDecoder.NAME, new HeaderDecoder(operationsFactory.getCodec(), channelFactory, configuration, operationsFactory.getListenerNotifier()));
      if (configuration.connectionPool().minEvictableIdleTime() > 0) {
         // This handler needs to be the last so that HeaderDecoder has the chance to cancel the idle event
         channel.pipeline().addLast(IdleStateHandlerProvider.NAME,
               new IdleStateHandlerProvider(configuration.connectionPool().minIdle(), channelPool));
      }
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

      channel.pipeline().addLast(AuthHandler.NAME, new AuthHandler(authentication, saslClient, operationsFactory));
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

   private class ActivationFuture extends CompletableFuture<Channel> implements ChannelFutureListener, BiConsumer<Channel, Throwable> {
      @Override
      public void operationComplete(ChannelFuture future) throws Exception {
         if (future.isSuccess()) {
            Channel channel = future.channel();
            ChannelRecord channelRecord = new ChannelRecord(unresolvedAddress, channelPool);
            channel.attr(ChannelRecord.KEY).set(channelRecord);
            channelRecord.whenComplete(this);
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
