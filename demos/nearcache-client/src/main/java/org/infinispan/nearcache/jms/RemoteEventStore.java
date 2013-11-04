package org.infinispan.nearcache.jms;

import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.persistence.remote.RemoteStore;
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
public class RemoteEventStore extends RemoteStore {

   private static final Log log = LogFactory.getLog(RemoteEventStore.class);

   private Connection con;

   @Override
   public void start() throws PersistenceException {
      try {
         Context context = getContext();
         try {
            MessageConsumer consumer = getMessageConsumer(context);
            consumer.setMessageListener(new RemoteEventListener(ctx.getCache(), ctx.getMarshaller()));
            log.infof("Subscribed to remote cache events");
         } finally {
            context.close();
         }
      } catch (Exception e) {
         throw new PersistenceException(
               "Unable to subscribe for remote cache events", e);
      }

      super.start(); // Connect to the remote store
   }

   @Override
   public void stop() throws PersistenceException {
      super.stop();
      if (con != null) {
         try {
            con.close();
         } catch (JMSException e) {
            throw new PersistenceException(
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

}
