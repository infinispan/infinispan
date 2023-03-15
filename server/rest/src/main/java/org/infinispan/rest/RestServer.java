package org.infinispan.rest;

import static org.infinispan.rest.RestChannelInitializer.MAX_HEADER_SIZE;
import static org.infinispan.rest.RestChannelInitializer.MAX_INITIAL_LINE_SIZE;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.infinispan.commons.logging.LogFactory;
import org.infinispan.counter.EmbeddedCounterManagerFactory;
import org.infinispan.counter.impl.manager.EmbeddedCounterManager;
import org.infinispan.rest.cachemanager.RestCacheManager;
import org.infinispan.rest.configuration.RestAuthenticationConfiguration;
import org.infinispan.rest.configuration.RestServerConfiguration;
import org.infinispan.rest.framework.ResourceManager;
import org.infinispan.rest.framework.RestDispatcher;
import org.infinispan.rest.framework.impl.ResourceManagerImpl;
import org.infinispan.rest.framework.impl.RestDispatcherImpl;
import org.infinispan.rest.resources.CacheResourceV2;
import org.infinispan.rest.resources.ClusterResource;
import org.infinispan.rest.resources.ContainerResource;
import org.infinispan.rest.resources.CounterResource;
import org.infinispan.rest.resources.LoggingResource;
import org.infinispan.rest.resources.MetricsResource;
import org.infinispan.rest.resources.ProtobufResource;
import org.infinispan.rest.resources.RedirectResource;
import org.infinispan.rest.resources.SearchAdminResource;
import org.infinispan.rest.resources.SecurityResource;
import org.infinispan.rest.resources.ServerResource;
import org.infinispan.rest.resources.StaticContentResource;
import org.infinispan.rest.resources.TasksResource;
import org.infinispan.rest.resources.XSiteResource;
import org.infinispan.rest.tracing.RestTelemetryService;
import org.infinispan.security.actions.SecurityActions;
import org.infinispan.server.core.AbstractProtocolServer;
import org.infinispan.server.core.logging.Log;
import org.infinispan.server.core.telemetry.TelemetryService;
import org.infinispan.server.core.transport.NettyInitializers;

import io.netty.channel.Channel;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOutboundHandler;
import io.netty.channel.group.ChannelMatcher;
import io.netty.handler.codec.http.cors.CorsConfig;

/**
 * REST Protocol Server.
 *
 * @author Sebastian Łaskawiec
 */
public class RestServer extends AbstractProtocolServer<RestServerConfiguration> {

   private static final Log log = LogFactory.getLog(RestServer.class, Log.class);
   private static final int CROSS_ORIGIN_ALT_PORT = 9000;

   private RestDispatcher restDispatcher;
   private RestCacheManager<Object> restCacheManager;
   private InvocationHelper invocationHelper;
   private volatile List<CorsConfig> corsRules;
   private volatile int maxContentLength;

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

   @Override
   public ChannelMatcher getChannelMatcher() {
      return channel -> channel.pipeline().get(RestRequestHandler.class) != null;
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

   public InvocationHelper getInvocationHelper() {
      return invocationHelper;
   }

   @Override
   public void stop() {
      if (log.isDebugEnabled())
         log.debugf("Stopping server %s listening at %s:%d", getQualifiedName(), configuration.host(), configuration.port());

      if (restCacheManager != null) {
         restCacheManager.stop();
      }
      RestAuthenticationConfiguration auth = configuration.authentication();
      if (auth.enabled()) {
         try {
            auth.authenticator().close();
         } catch (IOException e) {
            log.trace(e);
         }
      }
      super.stop();
   }

   @Override
   protected void startInternal() {
      TelemetryService telemetryService = SecurityActions.getGlobalComponentRegistry(cacheManager)
            .getComponent(TelemetryService.class);
      if (telemetryService == null) {
         telemetryService = new TelemetryService.NoTelemetry();
      }
      RestTelemetryService restTelemetryService = new RestTelemetryService(telemetryService);

      this.maxContentLength = configuration.maxContentLength() + MAX_INITIAL_LINE_SIZE + MAX_HEADER_SIZE;
      RestAuthenticationConfiguration auth = configuration.authentication();
      if (auth.enabled()) {
         auth.authenticator().init(this);
      }
      super.startInternal();
      restCacheManager = new RestCacheManager<>(cacheManager, this::isCacheIgnored);

      invocationHelper = new InvocationHelper(this, restCacheManager,
            (EmbeddedCounterManager) EmbeddedCounterManagerFactory.asCounterManager(cacheManager),
            configuration, server, getExecutor());

      String restContext = configuration.contextPath();
      String rootContext = "/";
      ResourceManager resourceManager = new ResourceManagerImpl();
      resourceManager.registerResource(restContext, new CacheResourceV2(invocationHelper, restTelemetryService));
      resourceManager.registerResource(restContext, new CounterResource(invocationHelper));
      resourceManager.registerResource(restContext, new ContainerResource(invocationHelper));
      resourceManager.registerResource(restContext, new XSiteResource(invocationHelper));
      resourceManager.registerResource(restContext, new SearchAdminResource(invocationHelper));
      resourceManager.registerResource(restContext, new TasksResource(invocationHelper));
      resourceManager.registerResource(restContext, new ProtobufResource(invocationHelper, restTelemetryService));
      resourceManager.registerResource(rootContext, new MetricsResource(auth.metricsAuth(), invocationHelper));
      Path staticResources = configuration.staticResources();
      if (staticResources != null) {
         Path console = configuration.staticResources().resolve("console");
         resourceManager.registerResource(rootContext, new StaticContentResource(invocationHelper, staticResources, "static"));
         resourceManager.registerResource(rootContext, new StaticContentResource(invocationHelper, console, "console", (path, resource) -> {
            if (!path.contains(".")) return StaticContentResource.DEFAULT_RESOURCE;
            return path;
         }));
         // if the cache name contains '.' we need to retrieve the console and access to the cache detail. See ISPN-14376
         resourceManager.registerResource(rootContext, new StaticContentResource(invocationHelper, console, "console/cache/", (path, resource) -> StaticContentResource.DEFAULT_RESOURCE));
         resourceManager.registerResource(rootContext, new RedirectResource(invocationHelper, rootContext, rootContext + "console/welcome", true));
      }
      if (adminEndpoint) {
         resourceManager.registerResource(restContext, new ServerResource(invocationHelper));
         resourceManager.registerResource(restContext, new ClusterResource(invocationHelper));
         resourceManager.registerResource(restContext, new  SecurityResource(invocationHelper, rootContext + "console/", rootContext + "console/forbidden.html"));
         registerLoggingResource(resourceManager, restContext);
      }
      this.restDispatcher = new RestDispatcherImpl(resourceManager, restCacheManager.getAuthorizer());
   }

   private void registerLoggingResource(ResourceManager resourceManager, String restContext) {
      String includeLoggingResource = System.getProperty("infinispan.server.resource.logging", "true");
      if (Boolean.parseBoolean(includeLoggingResource)) {
         resourceManager.registerResource(restContext, new LoggingResource(invocationHelper));
      }
   }

   public int maxContentLength() {
      return maxContentLength;
   }

   public List<CorsConfig> getCorsConfigs() {
      List<CorsConfig> rules = corsRules;
      if (rules == null) {
         synchronized (this) {
            rules = corsRules;
            if (rules == null) {
               rules = new ArrayList<>();
               rules.addAll(CorsUtil.enableAllForSystemConfig());
               rules.addAll(CorsUtil.enableAllForLocalHost(getPort(), CROSS_ORIGIN_ALT_PORT));
               rules.addAll(getConfiguration().getCorsRules());
               corsRules = rules;
            }
         }
      }
      return corsRules;
   }

   @Override
   public void installDetector(Channel ch) {
      // NO-OP
   }
}
