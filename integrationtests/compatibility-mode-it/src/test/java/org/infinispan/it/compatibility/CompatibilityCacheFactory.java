/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

package org.infinispan.it.compatibility;

import net.spy.memcached.MemcachedClient;
import org.apache.commons.httpclient.HttpClient;
import org.infinispan.Cache;
import org.infinispan.api.BasicCache;
import org.infinispan.api.BasicCacheContainer;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.Marshaller;
import org.infinispan.rest.ServerBootstrap;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.memcached.MemcachedServer;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.jboss.resteasy.plugins.server.servlet.HttpServletDispatcher;
import org.jboss.resteasy.plugins.server.servlet.ResteasyBootstrap;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.Context;

import javax.servlet.ServletContext;
import java.util.Collections;

import static org.infinispan.client.hotrod.TestHelper.startHotRodServer;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killRemoteCacheManager;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killServers;
import static org.infinispan.test.TestingUtil.killCacheManagers;
import static org.infinispan.server.memcached.test.MemcachedTestingUtil.startMemcachedTextServer;
import static org.infinispan.server.memcached.test.MemcachedTestingUtil.createMemcachedClient;
import static org.infinispan.server.memcached.test.MemcachedTestingUtil.killMemcachedClient;
import static org.infinispan.server.memcached.test.MemcachedTestingUtil.killMemcachedServer;

/**
 * Compatibility cache factory taking care of construction and destruction of
 * caches, servers and clients for each of the endpoints being tested.
 *
 * @author Galder Zamarreño
 * @since 5.3
 */
public class CompatibilityCacheFactory<K, V> {

   private EmbeddedCacheManager cacheManager;
   private HotRodServer hotrod;
   private RemoteCacheManager hotrodClient;
   private Server rest;
   private MemcachedServer memcached;

   private Cache<K, V> embeddedCache;
   private RemoteCache<K, V> hotrodCache;
   private HttpClient restClient;
   private MemcachedClient memcachedClient;

   private final String cacheName;
   private final Marshaller marshaller;
   private int restPort;

   CompatibilityCacheFactory() {
      this("", null);
   }

   CompatibilityCacheFactory(String cacheName, Marshaller marshaller) {
      this.cacheName = cacheName;
      this.marshaller = marshaller;
   }

   void setup() throws Exception {
      createEmbeddedCache();
      createHotRodCache();
      restPort = hotrod.getPort() + 25;
      createRestCache(restPort);
      createMemcachedCache();
   }

   void createEmbeddedCache() {
      org.infinispan.configuration.cache.ConfigurationBuilder builder =
            new org.infinispan.configuration.cache.ConfigurationBuilder();
      builder.compatibility().enable().marshaller(marshaller);
      cacheManager = TestCacheManagerFactory.createCacheManager(builder);
      embeddedCache = cacheName.isEmpty()
            ? cacheManager.<K, V>getCache()
            : cacheManager.<K, V>getCache(cacheName);
   }

   void createHotRodCache() {
      hotrod = startHotRodServer(cacheManager);
      hotrodClient = new RemoteCacheManager(new ConfigurationBuilder()
            .addServers("localhost:" + hotrod.getPort())
            .marshaller(marshaller)
            .build());
      hotrodCache = cacheName.isEmpty()
            ? hotrodClient.<K, V>getCache()
            : hotrodClient.<K, V>getCache(cacheName);
   }

   void createRestCache(int port) throws Exception {
      rest = new Server(port);
      Context ctx = new Context(rest, "/", Context.SESSIONS);
      ctx.setInitParams(Collections.singletonMap("resteasy.resources", "org.infinispan.rest.Server"));
      ctx.addEventListener(new ResteasyBootstrap());
      ctx.addServlet(HttpServletDispatcher.class, "/rest/*");
      ServletContext servletContext = ctx.getServletContext();
      ServerBootstrap.setCacheManager(servletContext, cacheManager);
      rest.start();
      restClient = new HttpClient();
   }

   private void createMemcachedCache() {
      memcached = startMemcachedTextServer(cacheManager);
      memcachedClient = createMemcachedClient(60000, memcached.getPort());
   }

   void teardown() {
      killRemoteCacheManager(hotrodClient);
      killServers(hotrod);
      killRestServer(rest);
      killMemcachedClient(memcachedClient);
      killMemcachedServer(memcached);
      killCacheManagers(cacheManager);
   }

   void killRestServer(Server rest) {
      if (rest != null) {
         try {
            rest.stop();
         } catch (Exception e) {
            // Ignore
         }
      }
   }

   Cache<K, V> getEmbeddedCache() {
      return embeddedCache;
   }

   RemoteCache<K, V> getHotRodCache() {
      return hotrodCache;
   }

   HttpClient getRestClient() {
      return restClient;
   }

   MemcachedClient getMemcachedClient() {
      return memcachedClient;
   }

   String getRestUrl() {
      String restCacheName = cacheName.isEmpty() ? BasicCacheContainer.DEFAULT_CACHE_NAME : cacheName;
      return String.format("http://localhost:%s/rest/%s", restPort, restCacheName);
   }

}
