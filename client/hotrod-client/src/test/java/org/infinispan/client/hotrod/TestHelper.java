/*
 * JBoss, Home of Professional Open Source
 * Copyright 2010 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.client.hotrod;

import org.infinispan.config.Configuration;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.test.HotRodTestingUtil;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.jboss.netty.channel.ChannelException;

import java.net.BindException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public class TestHelper {

   private static final Log log = LogFactory.getLog(TestHelper.class);

   /**
    * Holds unique Hot Rod server port
    */
   private static final ThreadLocal<Integer> uniquePort = new ThreadLocal<Integer>() {
      private final AtomicInteger uniquePort = new AtomicInteger(15232);

      @Override
      protected Integer initialValue() {
         return uniquePort.getAndAdd(25);
      }
   };

   public static HotRodServer startHotRodServer(EmbeddedCacheManager cacheManager) {
      // TODO: This is very rudimentary!! HotRodTestingUtil needs a more robust solution where ports are generated randomly and retries if already bound
      HotRodServer server = null;
      int maxTries = 5;
      int currentTries = 0;
      Integer port = uniquePort.get();
      ChannelException lastException = null;
      while (server == null && currentTries < maxTries) {
         try {
            server = HotRodTestingUtil.startHotRodServer(cacheManager, port++);
         } catch (ChannelException e) {
            if (!(e.getCause() instanceof BindException)) {
               throw e;
            } else {
               log.debug("Address already in use: [" + e.getMessage() + "], so let's try next port");
               currentTries++;
               lastException = e;
            }
         }
      }
      if (server == null && lastException != null)
         throw lastException;

      return server;
   }

   public static String getServersString(HotRodServer... servers) {
      StringBuilder builder = new StringBuilder();
      for (HotRodServer server : servers) {
         builder.append("localhost").append(':').append(server.getPort()).append(";");
      }
      return builder.toString();
   }

   public static Configuration getMultiNodeConfig() {
      Configuration result = new Configuration();
      result.setCacheMode(Configuration.CacheMode.DIST_SYNC);
      result.setSyncReplTimeout(10000);
//      result.setFetchInMemoryState(true);
      result.setSyncCommitPhase(true);
      result.setSyncRollbackPhase(true);
      return result;      
   }
}
