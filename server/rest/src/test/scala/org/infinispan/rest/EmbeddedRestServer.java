package org.infinispan.rest;

import java.util.Collections;

import javax.servlet.ServletContext;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.rest.configuration.RestServerConfiguration;
import org.jboss.resteasy.plugins.server.servlet.HttpServletDispatcher;
import org.jboss.resteasy.plugins.server.servlet.ResteasyBootstrap;
import org.mortbay.jetty.Connector;
import org.mortbay.jetty.nio.SelectChannelConnector;
import org.mortbay.jetty.servlet.Context;

public class EmbeddedRestServer {
   Context context;
   final EmbeddedCacheManager cacheManager;
   final String host;
   final int port;

   public EmbeddedRestServer(String host, int port, EmbeddedCacheManager cacheManager, RestServerConfiguration configuration) {
      this.host = host;
      this.port = port;
      this.cacheManager = cacheManager;
      init(configuration);
   }

   public EmbeddedRestServer(int port, EmbeddedCacheManager cacheManager, RestServerConfiguration configuration) {
      this("localhost", port, cacheManager, configuration);
   }

   private void init(RestServerConfiguration configuration) {
      org.mortbay.jetty.Server server = new org.mortbay.jetty.Server();
      Connector connector = new SelectChannelConnector();
      connector.setHost(host);
      connector.setPort(port);
      server.addConnector(connector);
      context = new Context(server, "/", Context.SESSIONS);
      context.setInitParams(Collections.singletonMap("resteasy.resources", "org.infinispan.rest.Server"));
      context.addEventListener(new ResteasyBootstrap());
      context.addServlet(HttpServletDispatcher.class, "/rest/*");
      ServletContext servletContext = context.getServletContext();
      ServerBootstrap.setCacheManager(servletContext, cacheManager);
      ServerBootstrap.setConfiguration(servletContext, configuration);
   }

   public void start() throws Exception {
      cacheManager.start();
      for (String cacheName : cacheManager.getCacheNames()) {
         cacheManager.getCache(cacheName);
      }
      cacheManager.getCache();
      context.getServer().start();
   }

   public void stop() throws Exception {
      context.getServer().stop();
      cacheManager.stop();
   }

   public EmbeddedCacheManager getCacheManager() {
      return cacheManager;
   }

   public String getHost() {
      return host;
   }

   public int getPort() {
      return port;
   }
}
