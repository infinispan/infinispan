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
