package org.infinispan.rest;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethodBase;
import org.apache.commons.httpclient.SimpleHttpConnectionManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.rest.configuration.RestServerConfiguration;
import org.infinispan.rest.configuration.RestServerConfigurationBuilder;

/**
 *
 * Basis for REST server tests.
 *
 * @author Michal Linhard (mlinhard@redhat.com)
 *
 */
public class RestServerTestBase {
   private Map<String, NettyRestServer> servers = new HashMap<String, NettyRestServer>();
   private HttpClient client;

   protected void createClient() {
      client = new HttpClient();
   }

   protected void destroyClient() {
      ((SimpleHttpConnectionManager) client.getHttpConnectionManager()).shutdown();
      client = null;
   }

   public void addServer(String name, int port, EmbeddedCacheManager cacheManager) {
      RestServerConfigurationBuilder builder = new RestServerConfigurationBuilder();
      builder.port(port);
      servers.put(name, NettyRestServer.apply(builder.build(), cacheManager));
   }

   public void addServer(String name, EmbeddedCacheManager cacheManager, RestServerConfiguration configuration) {
      servers.put(name, NettyRestServer.apply(configuration, cacheManager));
   }

   protected void removeServers() {
      servers.clear();
   }

   protected EmbeddedCacheManager getCacheManager(String name) {
      NettyRestServer ctx = servers.get(name);
      if (ctx == null) {
         return null;
      }
      return ctx.cacheManager();
   }

   public void startServers() throws Exception {
      if (!servers.isEmpty()) {
         for (NettyRestServer s : servers.values())
            s.start();
      } else {
         throw new IllegalStateException("No servers defined!");
      }
   }

   public void stopServers() throws Exception {
      if (!servers.isEmpty()) {
         for (NettyRestServer s : servers.values())
            s.stop();
      } else {
         throw new IllegalStateException("No servers defined!");
      }
   }

   protected HttpMethodBase call(HttpMethodBase method) throws Exception {
      client.executeMethod(method);
      return method;
   }
}