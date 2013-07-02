package org.infinispan.demo.nearcache.server;

import org.hornetq.integration.bootstrap.HornetQBootstrapServer;

import java.io.InputStream;

import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryRemoved;
import org.infinispan.notifications.cachelistener.event.CacheEntryEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryModifiedEvent;
import org.infinispan.server.core.ProtocolServer;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.Topic;
import javax.naming.Context;
import javax.naming.InitialContext;

/**
 * Hot Rod and JMS messaging server
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
public class MessagingHotRodDemo {

   private static final Log log = LogFactory.getLog(MessagingHotRodDemo.class);

   public static void main(String[] args) throws Exception {
      final InputStream in = System.in;

      // Start HornetQ server
      HornetQBootstrapServer.main(new String[]{"hornetq-beans.xml"});
      // Start Infinispan remote data grid, listening on localhost:11222
      HotRodServerConfigurationBuilder builder = new HotRodServerConfigurationBuilder();
      HotRodServer server = new HotRodServer();
      EmbeddedCacheManager cm = new DefaultCacheManager();
      server.start(builder.build(), cm);

      Context ctx = new InitialContext();
      Connection con = null;
      try {
         con = ((ConnectionFactory) ctx.lookup("/ConnectionFactory"))
               .createConnection();
         con.start(); // Start delivery

         // Add a message producer to send invalidation messages to near cache clients
         Session s = con.createSession(false, Session.AUTO_ACKNOWLEDGE);

         // Set up the datagrid topic (TODO: Selective key/cache topics)
         Topic topic = (Topic) ctx.lookup("/topic/datagrid");

         // Add invalidation listener for default cache
         cm.getCache().addListener(new InvalidationProducer(s, topic));
      } finally {
         while (in.read() != -1) {}
         ctx.close();
         if (con != null) con.close();
         server.stop();
         cm.stop();
         System.exit(0);
      }
   }

   @Listener
   public static class InvalidationProducer {

      final MessageProducer msgProducer;
      final Session s;
      final Topic topic;

      public InvalidationProducer(Session s, Topic topic) throws JMSException {
         this.s = s;
         this.topic = topic;
         msgProducer = s.createProducer(topic);
      }

      @CacheEntryModified
      public void invalidateModifiedEntry(
            CacheEntryModifiedEvent<byte[], byte[]> e) throws JMSException {
         if (e.isPre() && e.getValue() != null) {
            log.infof("Entry modified");
            invalidate(e);
         }
      }

      @CacheEntryRemoved
      public void invalidateRemovedEntry(
            CacheEntryEvent<byte[], byte[]> e) throws JMSException {
         if (e.isPre()) {
            log.infof("Entry removed");
            invalidate(e);
         }
      }

      private void invalidate(CacheEntryEvent<byte[], byte[]> e) throws JMSException {
         byte[] keyBytes = e.getKey();
         // Hot Rod keys are byte[], so send them as they are in a BytesMessage
         BytesMessage msg = s.createBytesMessage();
         // Create a message with the key that's been modified or removed
         msg.writeBytes(keyBytes);
         msgProducer.send(msg);
         log.infof("Send invalidation message %s", msg.getJMSMessageID());
      }

   }


}
