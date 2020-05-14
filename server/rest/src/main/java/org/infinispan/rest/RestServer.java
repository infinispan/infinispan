package org.infinispan.rest;

import java.io.IOException;
import java.nio.file.Path;

import org.infinispan.commons.logging.LogFactory;
import org.infinispan.counter.EmbeddedCounterManagerFactory;
import org.infinispan.counter.impl.manager.EmbeddedCounterManager;
import org.infinispan.rest.cachemanager.RestCacheManager;
import org.infinispan.rest.configuration.AuthenticationConfiguration;
import org.infinispan.rest.configuration.RestServerConfiguration;
import org.infinispan.rest.framework.ResourceManager;
import org.infinispan.rest.framework.RestDispatcher;
import org.infinispan.rest.framework.impl.ResourceManagerImpl;
import org.infinispan.rest.framework.impl.RestDispatcherImpl;
import org.infinispan.rest.resources.CacheManagerResource;
import org.infinispan.rest.resources.CacheResourceV2;
import org.infinispan.rest.resources.ClusterResource;
import org.infinispan.rest.resources.CounterResource;
import org.infinispan.rest.resources.LoggingResource;
import org.infinispan.rest.resources.LoginResource;
import org.infinispan.rest.resources.MetricsResource;
import org.infinispan.rest.resources.ProtobufResource;
import org.infinispan.rest.resources.RedirectResource;
import org.infinispan.rest.resources.SearchAdminResource;
import org.infinispan.rest.resources.ServerResource;
import org.infinispan.rest.resources.StaticContentResource;
import org.infinispan.rest.resources.TasksResource;
import org.infinispan.rest.resources.XSiteResource;
import org.infinispan.server.core.AbstractProtocolServer;
import org.infinispan.server.core.ServerManagement;
import org.infinispan.server.core.logging.Log;
import org.infinispan.server.core.transport.NettyInitializers;

import io.netty.channel.Channel;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOutboundHandler;

/**
 * REST Protocol Server.
 *
 * @author Sebastian Łaskawiec
 */
public class RestServer extends AbstractProtocolServer<RestServerConfiguration> {

   private static final Log log = LogFactory.getLog(RestServer.class, Log.class);

   private ServerManagement server;
   private RestDispatcher restDispatcher;
   private RestCacheManager<Object> restCacheManager;
   private InvocationHelper invocationHelper;

   public RestServer() {
      super("REST");
   }

   @Override
   public ChannelOutboundHandler getEncoder() {
      return null;
   }

   @Override
   public ChannelInboundHandler getDecoder() {
      return null;
   }

   @Override
   public ChannelInitializer<Channel> getInitializer() {
      return new NettyInitializers(getRestChannelInitializer());
   }

   /**
    * Returns Netty Channel Initializer for REST.
    *
    * @return Netty Channel Initializer for REST.
    */
   public RestChannelInitializer getRestChannelInitializer() {
      return new RestChannelInitializer(this, transport);
   }

   RestDispatcher getRestDispatcher() {
      return restDispatcher;
   }

   @Override
   public void stop() {
      if (log.isDebugEnabled())
         log.debugf("Stopping server %s listening at %s:%d", getQualifiedName(), configuration.host(), configuration.port());

      if (restCacheManager != null) {
         restCacheManager.stop();
      }
      AuthenticationConfiguration auth = configuration.authentication();
      if (auth.enabled()) {
         try {
            auth.authenticator().close();
         } catch (IOException e) {
            log.trace(e);
         }
      }
      super.stop();
   }

   public void setServer(ServerManagement server) {
      this.server = server;
   }

   @Override
   protected void startInternal() {
      AuthenticationConfiguration auth = configuration.authentication();
      if (auth.enabled()) {
         auth.authenticator().init(this);
      }
      super.startInternal();
      restCacheManager = new RestCacheManager<>(cacheManager, this::isCacheIgnored);

      invocationHelper = new InvocationHelper(restCacheManager,
            (EmbeddedCounterManager) EmbeddedCounterManagerFactory.asCounterManager(cacheManager),
            configuration, server, getExecutor());

      String restContext = configuration.contextPath();
      String rootContext = "/";
      ResourceManager resourceManager = new ResourceManagerImpl();
      resourceManager.registerResource(restContext, new CacheResourceV2(invocationHelper));
      resourceManager.registerResource(restContext, new CounterResource(invocationHelper));
      resourceManager.registerResource(restContext, new CacheManagerResource(invocationHelper));
      resourceManager.registerResource(restContext, new XSiteResource(invocationHelper));
      resourceManager.registerResource(restContext, new SearchAdminResource(invocationHelper));
      resourceManager.registerResource(restContext, new TasksResource(invocationHelper));
      resourceManager.registerResource(restContext, new ProtobufResource(invocationHelper));
      resourceManager.registerResource(rootContext, new MetricsResource());
      Path staticResources = configuration.staticResources();
      if (staticResources != null) {
         Path console = configuration.staticResources().resolve("console");
         resourceManager.registerResource(rootContext, new StaticContentResource(staticResources, "static"));
         resourceManager.registerResource(rootContext, new StaticContentResource(console, "console", (path, resource) -> {
            if (!path.contains(".")) return StaticContentResource.DEFAULT_RESOURCE;
            return path;
         }));
         resourceManager.registerResource(rootContext, new RedirectResource(rootContext, rootContext + "console/welcome", true));
      }
      if (server != null) {
         resourceManager.registerResource(restContext, new ServerResource(invocationHelper));
         resourceManager.registerResource(restContext, new ClusterResource(invocationHelper));
         resourceManager.registerResource(restContext, new LoginResource(invocationHelper, rootContext + "console/", rootContext + "console/forbidden.html"));
         resourceManager.registerResource(restContext, new LoggingResource(invocationHelper));
      }
      this.restDispatcher = new RestDispatcherImpl(resourceManager);
   }
}
