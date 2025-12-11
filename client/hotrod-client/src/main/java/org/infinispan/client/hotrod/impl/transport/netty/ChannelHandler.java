package org.infinispan.client.hotrod.impl.transport.netty;

import java.io.File;
import java.net.SocketAddress;
import java.security.Provider;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import org.infinispan.client.hotrod.TransportFactory;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.configuration.SslConfiguration;
import org.infinispan.client.hotrod.impl.ConfigurationProperties;
import org.infinispan.client.hotrod.impl.operations.HotRodOperation;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.io.FileWatcher;
import org.infinispan.commons.util.ProcessorInfo;
import org.infinispan.commons.util.SecurityProviders;
import org.infinispan.commons.util.SslContextFactory;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.IdentityCipherSuiteFilter;
import io.netty.handler.ssl.JdkSslContext;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.resolver.AddressResolverGroup;
import io.netty.resolver.dns.DnsNameResolverBuilder;
import io.netty.resolver.dns.RoundRobinDnsAddressResolverGroup;
import io.reactivex.rxjava3.core.Flowable;

public class ChannelHandler {
   private static final Log log = LogFactory.getLog(ChannelHandler.class);
   private final ConcurrentMap<SocketAddress, OperationChannel> channels = new ConcurrentHashMap<>();
   private final Function<SocketAddress, OperationChannel> newOpChannel = this::newOperationChannel;

   private final Configuration configuration;
   private final String sniHostName;
   private final EventLoopGroup eventLoopGroup;

   private final AddressResolverGroup<?> dnsResolver;
   private final SslContext sslContext;
   private final FileWatcher watcher;
   private final OperationDispatcher dispatcher;
   private final Consumer<ChannelPipeline> pipelineDecorator;

   public ChannelHandler(Configuration configuration, String sniHostName, ExecutorService executorService,
                         OperationDispatcher dispatcher, Consumer<ChannelPipeline> pipelineDecorator) {

      this.configuration = configuration;
      this.sniHostName = sniHostName;
      this.dispatcher = dispatcher;
      this.pipelineDecorator = pipelineDecorator;

      DnsNameResolverBuilder builder = new DnsNameResolverBuilder()
            .channelType(configuration.transportFactory().datagramChannelClass())
            .ttl(configuration.dnsResolverMinTTL(), configuration.dnsResolverMaxTTL())
            .negativeTtl(configuration.dnsResolverNegativeTTL());
      this.dnsResolver = new RoundRobinDnsAddressResolverGroup(builder);

      int asyncThreads = maxAsyncThreads(executorService, configuration);
      // static field with default is private in MultithreadEventLoopGroup
      int eventLoopThreads =
            Integer.getInteger("io.netty.eventLoopThreads", ProcessorInfo.availableProcessors() * 2);
      // Note that each event loop opens a selector which counts
      int maxExecutors = Math.min(asyncThreads, eventLoopThreads);
      this.eventLoopGroup = configuration.transportFactory().createEventLoopGroup(maxExecutors, executorService);

      SslConfiguration ssl = configuration.security().ssl();
      if (!ssl.enabled()) {
         this.sslContext = null;
         this.watcher = null;
      } else if (ssl.sslContext() == null) {
         this.sslContext = initSslContext(ssl);
         this.watcher = new FileWatcher();
      } else {
         this.sslContext = new JdkSslContext(ssl.sslContext(), true, null, IdentityCipherSuiteFilter.INSTANCE,
               null, ClientAuth.NONE, null, false);
         this.watcher = null;
      }

      configuration.metricRegistry().createGauge("connection.pool.size", "The total number of connections", channels::size, Map.of(), null);
   }

   public OperationChannel getOrCreateChannelForAddress(SocketAddress socketAddress) {
      OperationChannel operationChannel = channels.get(socketAddress);
      if (operationChannel == null) {
         operationChannel = channels.computeIfAbsent(socketAddress, newOpChannel);
      }
      return operationChannel;
   }

   public CompletionStage<Void> startChannelIfNeeded(SocketAddress socketAddress) {
      OperationChannel operationChannel = channels.computeIfAbsent(socketAddress, newOpChannel);
      return operationChannel.attemptConnect();
   }

   public OperationChannel removeChannel(SocketAddress address) {
      log.tracef("Removing OperationChannel for %s", address);
      return channels.remove(address);
   }

   public List<HotRodOperation<?>> closeChannel(SocketAddress address) {
      OperationChannel channel = removeChannel(address);
      if (channel != null) {
         log.tracef("Closing channel for %s", address);
         return channel.close();
      }
      return List.of();
   }

   public void close() {
      try {
         if (watcher != null) {
            watcher.stop();
         }
         // We only want to shutdown the EventLoop when using the default TransportFactory. This way users can control
         // the lifecycle of the EventLoop themselves.
         if (configuration.transportFactory() == TransportFactory.DEFAULT) {
            eventLoopGroup.shutdownGracefully(0, 0, TimeUnit.MILLISECONDS).get();
         }
      } catch (Exception e) {
         log.warn("Exception while shutting down the channel handler.", e);
      }
   }

   private SslContext initSslContext(SslConfiguration ssl) {
      SslContextBuilder builder = SslContextBuilder.forClient();
      try {
         if (ssl.keyStoreFileName() != null) {
            builder.keyManager(new SslContextFactory()
                  .keyStoreFileName(ssl.keyStoreFileName())
                  .keyStoreType(ssl.keyStoreType())
                  .keyStorePassword(ssl.keyStorePassword())
                  .keyAlias(ssl.keyAlias())
                  .classLoader(configuration.classLoader())
                  .provider(ssl.provider())
                  .watcher(watcher)
                  .build().keyManager());
         }
         if (ssl.trustStoreFileName() != null) {
            if ("pem".equalsIgnoreCase(ssl.trustStoreType())) {
               builder.trustManager(new File(ssl.trustStoreFileName()));
            } else {
               builder.trustManager(new SslContextFactory()
                     .trustStoreFileName(ssl.trustStoreFileName())
                     .trustStoreType(ssl.trustStoreType())
                     .trustStorePassword(ssl.trustStorePassword())
                     .classLoader(configuration.classLoader())
                     .provider(ssl.provider())
                     .watcher(watcher)
                     .build()
                     .trustManager());
            }
         }
         if (ssl.trustStorePath() != null) {
            builder.trustManager(new File(ssl.trustStorePath()));
         }
         if (ssl.protocol() != null) {
            builder.protocols(ssl.protocol());
         }
         if (ssl.ciphers() != null) {
            builder.ciphers(ssl.ciphers());
         }
         if (ssl.provider() != null) {
            Provider provider = SecurityProviders.findProvider(ssl.provider(), SslContext.class.getSimpleName(), "TLS");
            builder.sslContextProvider(provider);
         }
         return builder.build();
      } catch (Exception e) {
         throw new CacheConfigurationException(e);
      }
   }

   private int maxAsyncThreads(ExecutorService executorService, Configuration configuration) {
      if (executorService instanceof ThreadPoolExecutor) {
         return ((ThreadPoolExecutor) executorService).getMaximumPoolSize();
      }
      // Note: this is quite dangerous, if someone sets different executor factory and does not update this setting
      // we might deadlock
      return new ConfigurationProperties(configuration.asyncExecutorFactory().properties()).getDefaultExecutorFactoryPoolSize();
   }

   private OperationChannel newOperationChannel(SocketAddress address) {
      log.debugf("Creating new channel pool for %s", address);
      Bootstrap bootstrap = new Bootstrap()
            .group(eventLoopGroup)
            .channel(configuration.transportFactory().socketChannelClass())
            .resolver(dnsResolver)
            .remoteAddress(address)
            .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, configuration.connectionTimeout())
            .option(ChannelOption.SO_KEEPALIVE, configuration.tcpKeepAlive())
            .option(ChannelOption.TCP_NODELAY, configuration.tcpNoDelay())
            .option(ChannelOption.SO_RCVBUF, 1024576);
      ChannelInitializer channelInitializer = createChannelInitializer(address, bootstrap);
      bootstrap.handler(channelInitializer);
      OperationChannel operationChannel = createOperationChannel(channelInitializer, address);
      return operationChannel;
   }

   public ChannelInitializer createChannelInitializer(SocketAddress address, Bootstrap bootstrap) {
      return new ChannelInitializer(bootstrap, address, configuration, sniHostName, sslContext, dispatcher,
            pipelineDecorator);
   }

   protected OperationChannel createOperationChannel(ChannelInitializer channelInitializer, SocketAddress address) {
      return OperationChannel.createAndStart(address, channelInitializer, dispatcher::getClientTopologyInfo, dispatcher::handleConnectionFailure);
   }

   public Flowable<HotRodOperation<?>> pendingOperationFlowable() {
      return Flowable.fromIterable(channels.values())
            .flatMap(OperationChannel::pendingOperationFlowable);
   }

   public Stream<HotRodOperation<?>> gatherOperations() {
      return channels.values().stream().flatMap(oc ->
            oc.getChannel() != null ?
                  oc.getChannel().pipeline()
                        .get(HeaderDecoder.class)
                        .registeredOperationsById()
                        .values().stream()
                  : Stream.empty()
      );
   }

   public EventLoopGroup getEventLoopGroup() {
      return eventLoopGroup;
   }

   public OperationChannel getChannelForAddress(SocketAddress socketAddress) {
      return channels.get(socketAddress);
   }
}
