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
package org.infinispan.rest;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethodBase;
import org.apache.commons.httpclient.SimpleHttpConnectionManager;
import org.infinispan.Cache;
import org.infinispan.manager.AbstractDelegatingEmbeddedCacheManager;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.jboss.resteasy.plugins.server.servlet.HttpServletDispatcher;
import org.jboss.resteasy.plugins.server.servlet.ResteasyBootstrap;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;
import org.testng.AssertJUnit;

/**
 * 
 * Manages (possibly multiple) server instances.
 * 
 * @author Michal Linhard (mlinhard@redhat.com)
 * 
 */
public class ServerInstance {
   private static org.mortbay.jetty.Server singleServer;
   private static Map<String, org.mortbay.jetty.Server> multiServers = new HashMap<String, org.mortbay.jetty.Server>();
   private static Map<String, String> multiConfigs = new HashMap<String, String>();

   public static void addSingleServer(int port, String configFile) {
      singleServer = addStartupListener(createRESTEndpoint(port), configFile).getServer();
   }

   public static void addMultiServer(String name, int port, String configFile) {
      multiServers.put(name, createRESTEndpoint(port).getServer());
      multiConfigs.put(name, configFile);
   }

   public static void removeServers() {
      singleServer = null;
      multiServers.clear();
      multiConfigs.clear();
   }

   private static Context createRESTEndpoint(int port) {
      Context ctx = new Context(new org.mortbay.jetty.Server(port), "/", Context.SESSIONS);
      ctx.setInitParams(Collections.singletonMap("resteasy.resources", "org.infinispan.rest.Server"));
      ctx.addEventListener(new ResteasyBootstrap());
      ctx.addServlet(HttpServletDispatcher.class, "/rest/*");
      return ctx;
   }

   private static Context addStartupListener(Context ctx, String cfgFile) {
      ServletHolder sh = new ServletHolder(StartupListener.class);
      sh.setInitOrder(1);
      sh.setInitParameter("infinispan.config", cfgFile);
      ctx.addServlet(sh, "/listener/*");
      return ctx;
   }

   public static boolean started() {
      if (singleServer != null) {
         return singleServer.isStarted();
      } else if (!multiServers.isEmpty()) {
         for (org.mortbay.jetty.Server s : multiServers.values()) {
            if (!s.isStarted()) {
               return false;
            }
         }
         return true;
      } else {
         return false;
      }
   }

   public static void start() throws Exception {
      if (singleServer != null) {
         singleServer.start();
      } else if (!multiServers.isEmpty()) {
         Map<String, EmbeddedCacheManager> managers = new HashMap<String, EmbeddedCacheManager>();
         for (Map.Entry<String, String> multiServer : multiConfigs.entrySet()) {
            managers.put(multiServer.getKey(), new DefaultCacheManager(multiServer.getValue()));
         }
         for (EmbeddedCacheManager manager : managers.values()) {
            manager.start();
            for (String cacheName : manager.getCacheNames()) {
               manager.getCache(cacheName);
            }
            manager.getCache();
         }
         ManagerInstance.instance_$eq(new MultiDefaultCacheManager(managers.values().iterator().next(), managers));
         for (org.mortbay.jetty.Server server : multiServers.values()) {
            server.start();
         }
      } else {
         throw new IllegalStateException("No servers defined!");
      }
   }

   public static void stop() throws Exception {
      if (singleServer != null) {
         singleServer.stop();
         // ManagerInstance will be destroyed by StartupListener
      } else if (!multiServers.isEmpty()) {
         for (org.mortbay.jetty.Server s : multiServers.values()) {
            s.stop();
         }
         ManagerInstance.instance().stop();
         ManagerInstance.instance_$eq(null);
      } else {
         throw new IllegalStateException("No servers defined!");
      }
   }

   private static class MultiDefaultCacheManager extends AbstractDelegatingEmbeddedCacheManager {
      public static final String DELIM = "-";

      protected Map<String, EmbeddedCacheManager> managers = new HashMap<String, EmbeddedCacheManager>();

      public MultiDefaultCacheManager(EmbeddedCacheManager defaultManager, Map<String, EmbeddedCacheManager> managers) {
         super(defaultManager);
         this.managers.putAll(managers);
      }

      @Override
      public <K, V> Cache<K, V> getCache(String cacheName) {
         String[] tok = cacheName.split(DELIM);
         if (tok.length != 2) {
            return null;
         }
         EmbeddedCacheManager mgr = managers.get(tok[0]);
         if (mgr == null) {
            return null;
         } else {
            if (DEFAULT_CACHE_NAME.equals(cacheName)) {
               return mgr.getCache();
            } else {
               return mgr.getCache(tok[1]);
            }
         }
      }

      @Override
      public Set<String> getCacheNames() {
         Set<String> s = new TreeSet<String>();
         for (Map.Entry<String, EmbeddedCacheManager> entry : managers.entrySet()) {
            for (String cacheName : entry.getValue().getCacheNames()) {
               s.add(entry.getKey() + DELIM + cacheName);
            }
            s.add(entry.getKey() + DELIM + DEFAULT_CACHE_NAME);
         }
         return s;
      }
   }

   public static class Client {
      private static HttpClient client;

      public static HttpMethodBase call(HttpMethodBase method) throws Exception {
         AssertJUnit.assertTrue(ServerInstance.started());
         client.executeMethod(method);
         return method;
      }

      public static void create() {
         client = new HttpClient();
      }

      public static void destroy() {
         ((SimpleHttpConnectionManager) client.getHttpConnectionManager()).shutdown();
         client = null;
      }
   }
}