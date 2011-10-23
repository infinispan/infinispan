/*
 * Copyright 2011 Red Hat, Inc. and/or its affiliates.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA
 */

package org.infinispan.nearcache.jms;

import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.remote.RemoteCacheStore;
import org.infinispan.loaders.remote.RemoteCacheStoreConfig;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.Topic;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.util.Properties;

/**
 * A remote cache store that registers itself to listen for remote cache events
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
public class RemoteEventCacheStore extends RemoteCacheStore {

   private static final Log log = LogFactory.getLog(RemoteEventCacheStore.class);

   private Connection con;

   @Override
   public void start() throws CacheLoaderException {
      try {
         Context ctx = getContext();
         try {
            MessageConsumer consumer = getMessageConsumer(ctx);
            consumer.setMessageListener(new RemoteEventListener(cache, marshaller));
            log.infof("Subscribed to remote cache events");
         } finally {
            ctx.close();
         }
      } catch (Exception e) {
         throw new CacheLoaderException(
               "Unable to subscribe for remote cache events", e);
      }

      super.start(); // Connect to the remote store
   }

   @Override
   public void stop() throws CacheLoaderException {
      if (con != null) {
         try {
            con.close();
         } catch (JMSException e) {
            throw new CacheLoaderException(
                  "Unable to close remote cache event connection", e);
         }
      }
   }

   private MessageConsumer getMessageConsumer(Context ctx) throws Exception {
      ConnectionFactory cf = (ConnectionFactory) ctx.lookup("/ConnectionFactory");
      con = cf.createConnection();
      con.start(); // Start delivery

      Session s = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Topic topic = (Topic) ctx.lookup("/topic/datagrid");
      return s.createConsumer(topic);
   }

   private static Context getContext() throws NamingException {
      // TODO: Verify whether this works at all, otherwise fallback on Netty based pure HornetQ client?
      Properties p = new Properties();
      p.setProperty("java.naming.factory.initial",
                    "org.jnp.interfaces.NamingContextFactory");
      p.setProperty("java.naming.provider.url", "jnp://localhost:1099");
      p.setProperty("java.naming.factory.url.pkgs",
                    "org.jboss.naming:org.jnp.interfaces");
      return new InitialContext(p);
   }

   public static class RemoteEventCacheStoreConfig extends RemoteCacheStoreConfig {

      public RemoteEventCacheStoreConfig() {
         setCacheLoaderClassName(RemoteEventCacheStore.class.getName());
         // For demo simplicity, even if it's a named cache, store in default cache
         setUseDefaultRemoteCache(true);
      }

   }
}
