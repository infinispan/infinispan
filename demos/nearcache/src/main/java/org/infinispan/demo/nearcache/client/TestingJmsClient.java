package org.infinispan.demo.nearcache.client;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.util.Properties;

/**
 * Testing client
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
public class TestingJmsClient {

   public static void main(String[] args) throws Exception {
      Context ctx = getContext();
      Connection con = null;
      try {
         ConnectionFactory cf = (ConnectionFactory)
               ctx.lookup("/ConnectionFactory");
         con = cf.createConnection();
         Session s = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Topic topic = (Topic) ctx.lookup("/topic/datagrid");
         MessageProducer msgProduce = s.createProducer(topic);
         MessageConsumer msgConsume1 = s.createConsumer(topic);
         MessageConsumer msgConsume2 = s.createConsumer(topic);
         TextMessage message = s.createTextMessage("This is a text message");
         msgProduce.send(message);
         con.start();
         TextMessage msgReceived = (TextMessage) msgConsume1.receive();
         System.out.println(msgReceived.getText());
         msgReceived = (TextMessage) msgConsume2.receive();
         System.out.println(msgReceived.getText());
      } finally {
         ctx.close();
         if (con != null)
            con.close();
      }
   }

   private static Context getContext() throws NamingException {
      Properties p = new Properties();
      p.setProperty("java.naming.factory.initial",
                    "org.jnp.interfaces.NamingContextFactory");
      p.setProperty("java.naming.provider.url", "jnp://localhost:1099");
      p.setProperty("java.naming.factory.url.pkgs",
                    "org.jboss.naming:org.jnp.interfaces");
      return new InitialContext(p);
   }

}
