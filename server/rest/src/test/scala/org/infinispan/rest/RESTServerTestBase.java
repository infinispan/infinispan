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
class RESTServerTestBase {
   private Map<String, Context> servers = new HashMap<String, Context>();
   private HttpClient client;

   protected void createClient() {
      client = new HttpClient();
   }

   protected void destroyClient() {
      ((SimpleHttpConnectionManager) client.getHttpConnectionManager()).shutdown();
      client = null;
   }

   protected void addServer(String name, int port, EmbeddedCacheManager cacheManager) {
      servers.put(name, createRESTEndpoint(port, cacheManager, new RestServerConfigurationBuilder().build()));
   }

   protected void addServer(String name, int port, EmbeddedCacheManager cacheManager, RestServerConfiguration configuration) {
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

   protected void startServers() throws Exception {
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

   protected void stopServers() throws Exception {
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