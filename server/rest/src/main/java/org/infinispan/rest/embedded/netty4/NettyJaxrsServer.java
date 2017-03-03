package org.infinispan.rest.embedded.netty4;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;

import org.infinispan.rest.embedded.netty4.security.Authenticator;
import org.jboss.resteasy.core.SynchronousDispatcher;
import org.jboss.resteasy.plugins.server.embedded.EmbeddedJaxrsServer;
import org.jboss.resteasy.plugins.server.embedded.SecurityDomain;
import org.jboss.resteasy.spi.ResteasyDeployment;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.netty.handler.ssl.SniHandler;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.EventExecutor;

/**
 * An HTTP server that sends back the content of the received HTTP request
 * in a pretty plaintext form.
 *
 * @author <a href="http://www.jboss.org/netty/">The Netty Project</a>
 * @author Andy Taylor (andy.taylor@jboss.org)
 * @author <a href="http://gleamynode.net/">Trustin Lee</a>
 * @author Norman Maurer
 * @version $Rev: 2080 $, $Date: 2010-01-26 18:04:19 +0900 (Tue, 26 Jan 2010) $
 * Temporary fork from RestEasy 3.1.0
 */
public class NettyJaxrsServer implements EmbeddedJaxrsServer {
   protected ServerBootstrap bootstrap = new ServerBootstrap();
   protected String hostname = null;
   protected int configuredPort = 8080;
   protected int runtimePort = -1;
   protected ResteasyDeployment deployment = new ResteasyDeployment();
   protected String root = "";
   protected SecurityDomain domain;
   protected Authenticator authenticator;
   private EventLoopGroup eventLoopGroup;
   private EventLoopGroup eventExecutor;
   private int ioWorkerCount = Runtime.getRuntime().availableProcessors() * 2;
   private int executorThreadCount = 16;
   private SSLContext sslContext;
   private SniConfiguration sniConfiguration;
   private int maxRequestSize = 1024 * 1024 * 10;
   private int maxInitialLineLength = 4096;
   private int maxHeaderSize = 8192;
   private int maxChunkSize = 8192;
   private int backlog = 128;
   // default no idle timeout.
   private int idleTimeout = -1;
   private List<ChannelHandler> channelHandlers = Collections.emptyList();
   private Map<ChannelOption, Object> channelOptions = Collections.emptyMap();
   private Map<ChannelOption, Object> childChannelOptions = Collections.emptyMap();
   private List<ChannelHandler> httpChannelHandlers = Collections.emptyList();

   public void setSSLContext(SSLContext sslContext) {
      this.sslContext = sslContext;
   }

   public SniConfiguration getSniConfiguration() {
      return sniConfiguration;
   }

   public void setSniConfiguration(SniConfiguration sniConfiguration) {
      this.sniConfiguration = sniConfiguration;
   }

   /**
    * Specify the worker count to use. For more information about this please see the javadocs of {@link EventLoopGroup}
    *
    * @param ioWorkerCount
    */
   public void setIoWorkerCount(int ioWorkerCount) {
      this.ioWorkerCount = ioWorkerCount;
   }

   /**
    * Set the number of threads to use for the EventExecutor. For more information please see the javadocs of {@link
    * EventExecutor}. If you want to disable the use of the {@link EventExecutor} specify a value <= 0.  This should
    * only be done if you are 100% sure that you don't have any blocking code in there.
    *
    * @param executorThreadCount
    */
   public void setExecutorThreadCount(int executorThreadCount) {
      this.executorThreadCount = executorThreadCount;
   }

   /**
    * Set the max. request size in bytes. If this size is exceed we will send a "413 Request Entity Too Large" to the
    * client.
    *
    * @param maxRequestSize the max request size. This is 10mb by default.
    */
   public void setMaxRequestSize(int maxRequestSize) {
      this.maxRequestSize = maxRequestSize;
   }

   public void setMaxInitialLineLength(int maxInitialLineLength) {
      this.maxInitialLineLength = maxInitialLineLength;
   }

   public void setMaxHeaderSize(int maxHeaderSize) {
      this.maxHeaderSize = maxHeaderSize;
   }

   public void setMaxChunkSize(int maxChunkSize) {
      this.maxChunkSize = maxChunkSize;
   }

   public String getHostname() {
      return hostname;
   }

   public void setHostname(String hostname) {
      this.hostname = hostname;
   }

   public int getPort() {
      return runtimePort > 0 ? runtimePort : configuredPort;
   }

   public void setPort(int port) {
      this.configuredPort = port;
   }

   public void setBacklog(int backlog) {
      this.backlog = backlog;
   }

   public int getIdleTimeout() {
      return idleTimeout;
   }

   /**
    * Set the idle timeout.
    * Set this value to turn on idle connection cleanup.
    * If there is no traffic within idleTimeoutSeconds, it'll close connection.
    *
    * @param idleTimeoutSeconds - How many seconds to cleanup client connection. default value -1 meaning no idle
    *                           timeout.
    */
   public void setIdleTimeout(int idleTimeoutSeconds) {
      this.idleTimeout = idleTimeoutSeconds;
   }

   /**
    * Add additional {@link io.netty.channel.ChannelHandler}s to the {@link io.netty.bootstrap.ServerBootstrap}.
    * <p>The additional channel handlers are being added <em>before</em> the HTTP handling.</p>
    *
    * @param channelHandlers the additional {@link io.netty.channel.ChannelHandler}s.
    */
   public void setChannelHandlers(final List<ChannelHandler> channelHandlers) {
      this.channelHandlers = channelHandlers == null ? Collections.<ChannelHandler>emptyList() : channelHandlers;
   }

   /**
    * Add additional {@link io.netty.channel.ChannelHandler}s to the {@link io.netty.bootstrap.ServerBootstrap}.
    * <p>The additional channel handlers are being added <em>after</em> the HTTP handling.</p>
    *
    * @param httpChannelHandlers the additional {@link io.netty.channel.ChannelHandler}s.
    */
   public void setHttpChannelHandlers(final List<ChannelHandler> httpChannelHandlers) {
      this.httpChannelHandlers = httpChannelHandlers == null ? Collections.<ChannelHandler>emptyList() : httpChannelHandlers;
   }

   /**
    * Add Netty {@link io.netty.channel.ChannelOption}s to the {@link io.netty.bootstrap.ServerBootstrap}.
    *
    * @param channelOptions the additional {@link io.netty.channel.ChannelOption}s.
    * @see io.netty.bootstrap.ServerBootstrap#option(io.netty.channel.ChannelOption, Object)
    */
   public void setChannelOptions(final Map<ChannelOption, Object> channelOptions) {
      this.channelOptions = channelOptions == null ? Collections.<ChannelOption, Object>emptyMap() : channelOptions;
   }

   /**
    * Add child options to the {@link io.netty.bootstrap.ServerBootstrap}.
    *
    * @param channelOptions the additional child {@link io.netty.channel.ChannelOption}s.
    * @see io.netty.bootstrap.ServerBootstrap#childOption(io.netty.channel.ChannelOption, Object)
    */
   public void setChildChannelOptions(final Map<ChannelOption, Object> channelOptions) {
      this.childChannelOptions = channelOptions == null ? Collections.<ChannelOption, Object>emptyMap() : channelOptions;
   }

   @Override
   public void setRootResourcePath(String rootResourcePath) {
      root = rootResourcePath;
      if (root != null && root.equals("/")) root = "";
   }

   @Override
   public ResteasyDeployment getDeployment() {
      return deployment;
   }

   @Override
   public void setDeployment(ResteasyDeployment deployment) {
      this.deployment = deployment;
   }

   @Override
   public void setSecurityDomain(SecurityDomain sc) {
      this.domain = sc;
   }

   public Authenticator getAuthenticator() {
      return authenticator;
   }

   public void setAuthenticator(Authenticator authenticator) {
      this.authenticator = authenticator;
   }

   protected RequestDispatcher createRequestDispatcher(boolean secure) {
      return new RequestDispatcher((SynchronousDispatcher) deployment.getDispatcher(), deployment.getProviderFactory(), authenticator);
   }

   @SuppressWarnings("unchecked")
   @Override
   public void start() {
      eventLoopGroup = new NioEventLoopGroup(ioWorkerCount);
      eventExecutor = new NioEventLoopGroup(executorThreadCount);
      deployment.start();
      // Configure the server.
      bootstrap.group(eventLoopGroup)
            .channel(NioServerSocketChannel.class)
            .childHandler(createChannelInitializer())
            .option(ChannelOption.SO_BACKLOG, backlog)
            .childOption(ChannelOption.SO_KEEPALIVE, true);

      for (Map.Entry<ChannelOption, Object> entry : channelOptions.entrySet()) {
         bootstrap.option(entry.getKey(), entry.getValue());
      }

      for (Map.Entry<ChannelOption, Object> entry : childChannelOptions.entrySet()) {
         bootstrap.childOption(entry.getKey(), entry.getValue());
      }

      final InetSocketAddress socketAddress;
      if (null == hostname || hostname.isEmpty()) {
         socketAddress = new InetSocketAddress(configuredPort);
      } else {
         socketAddress = new InetSocketAddress(hostname, configuredPort);
      }

      Channel channel = bootstrap.bind(socketAddress).syncUninterruptibly().channel();
      runtimePort = ((InetSocketAddress) channel.localAddress()).getPort();
   }

   private ChannelInitializer<SocketChannel> createChannelInitializer() {
      if (sslContext == null && sniConfiguration == null) {
         return new ChannelInitializer<SocketChannel>() {
            @Override
            public void initChannel(SocketChannel ch) throws Exception {
               setupHandlers(ch, createRequestDispatcher(false), RestEasyHttpRequestDecoder.Protocol.HTTP);
            }
         };
      } else if (sniConfiguration == null) {
         return new ChannelInitializer<SocketChannel>() {
            @Override
            public void initChannel(SocketChannel ch) throws Exception {
               SSLEngine engine = sslContext.createSSLEngine();
               engine.setUseClientMode(false);
               ch.pipeline().addFirst(new SslHandler(engine));
               setupHandlers(ch, createRequestDispatcher(true), RestEasyHttpRequestDecoder.Protocol.HTTPS);
            }
         };
      } else {
         return new ChannelInitializer<SocketChannel>() {
            @Override
            public void initChannel(SocketChannel ch) throws Exception {
               ch.pipeline().addFirst(new SniHandler(sniConfiguration.buildMapping()));
               setupHandlers(ch, createRequestDispatcher(true), RestEasyHttpRequestDecoder.Protocol.HTTPS);
            }
         };
      }
   }

   private void setupHandlers(SocketChannel ch, RequestDispatcher dispatcher, RestEasyHttpRequestDecoder.Protocol protocol) {
      ChannelPipeline channelPipeline = ch.pipeline();
      channelPipeline.addLast(channelHandlers.toArray(new ChannelHandler[channelHandlers.size()]));
      channelPipeline.addLast(new HttpRequestDecoder(maxInitialLineLength, maxHeaderSize, maxChunkSize));
      channelPipeline.addLast(new HttpObjectAggregator(maxRequestSize));
      channelPipeline.addLast(new HttpResponseEncoder());
      channelPipeline.addLast(httpChannelHandlers.toArray(new ChannelHandler[httpChannelHandlers.size()]));
      channelPipeline.addLast(new RestEasyHttpRequestDecoder(dispatcher.getDispatcher(), root, protocol));
      channelPipeline.addLast(new RestEasyHttpResponseEncoder());
      if (idleTimeout > 0) {
         channelPipeline.addLast("idleStateHandler", new IdleStateHandler(0, 0, idleTimeout));
      }
      channelPipeline.addLast(eventExecutor, new RequestHandler(dispatcher));
   }

   @Override
   public void stop() {
      runtimePort = -1;
      eventLoopGroup.shutdownGracefully().awaitUninterruptibly();
      eventExecutor.shutdownGracefully().awaitUninterruptibly();
   }
}
