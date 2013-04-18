package org.infinispan.rest;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.ServletContext;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethodBase;
import org.apache.commons.httpclient.SimpleHttpConnectionManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.rest.configuration.RestServerConfiguration;
import org.infinispan.rest.configuration.RestServerConfigurationBuilder;
import org.jboss.resteasy.plugins.server.servlet.HttpServletDispatcher;
import org.jboss.resteasy.plugins.server.servlet.ResteasyBootstrap;
import org.mortbay.jetty.servlet.Context;
import org.testng.AssertJUnit;

/**
 *
 * Basis for REST server tests.
 *
 * @author Michal Linhard (mlinhard@redhat.com)
 *
 */
public class RestServerTestBase {
   private Map<String, Context> servers = new HashMap<String, Context>();
   private HttpClient client;

   protected void createClient() {
      client = new HttpClient();
   }

   protected void destroyClient() {
      ((SimpleHttpConnectionManager) client.getHttpConnectionManager()).shutdown();
      client = null;
   }

   public void addServer(String name, int port, EmbeddedCacheManager cacheManager) {
      servers.put(name, createRESTEndpoint(port, cacheManager, new RestServerConfigurationBuilder().build()));
   }

   public void addServer(String name, int port, EmbeddedCacheManager cacheManager, RestServerConfiguration configuration) {
      servers.put(name, createRESTEndpoint(port, cacheManager, configuration));
   }

   protected void removeServers() {
      servers.clear();
   }

   protected EmbeddedCacheManager getCacheManager(String name) {
      Context ctx = servers.get(name);
      if (ctx == null) {
         return null;
      }
      return ServerBootstrap.getCacheManager(ctx.getServletContext());
   }

   protected ManagerInstance getManagerInstance(String name) {
      Context ctx = servers.get(name);
      if (ctx == null) {
         return null;
      }
      return ServerBootstrap.getManagerInstance(ctx.getServletContext());
   }

   protected Context createRESTEndpoint(int port, EmbeddedCacheManager cacheManager, RestServerConfiguration configuration) {
      Context ctx = new Context(new org.mortbay.jetty.Server(port), "/", Context.SESSIONS);
      ctx.setInitParams(Collections.singletonMap("resteasy.resources", "org.infinispan.rest.Server"));
      ctx.addEventListener(new ResteasyBootstrap());
      ctx.addServlet(HttpServletDispatcher.class, "/rest/*");
      ServletContext servletContext = ctx.getServletContext();
      ServerBootstrap.setCacheManager(servletContext, cacheManager);
      ServerBootstrap.setConfiguration(servletContext, configuration);
      return ctx;
   }

   protected boolean serversStarted() {
      if (!servers.isEmpty()) {
         for (Context s : servers.values()) {
            if (!s.getServer().isStarted()) {
               return false;
            }
         }
         return true;
      } else {
         return false;
      }
   }

   public void startServers() throws Exception {
      if (!servers.isEmpty()) {
         for (Context s : servers.values()) {
            EmbeddedCacheManager manager = ServerBootstrap.getCacheManager(s.getServletContext());
            manager.start();
            for (String cacheName : manager.getCacheNames()) {
               manager.getCache(cacheName);
            }
            manager.getCache();
            s.getServer().start();
         }
      } else {
         throw new IllegalStateException("No servers defined!");
      }
   }

   public void stopServers() throws Exception {
      if (!servers.isEmpty()) {
         for (Context s : servers.values()) {
            EmbeddedCacheManager manager = ServerBootstrap.getCacheManager(s.getServletContext());
            s.getServer().stop();
            manager.stop();
         }
      } else {
         throw new IllegalStateException("No servers defined!");
      }
   }

   protected HttpMethodBase call(HttpMethodBase method) throws Exception {
      AssertJUnit.assertTrue(serversStarted());
      client.executeMethod(method);
      return method;
   }
}