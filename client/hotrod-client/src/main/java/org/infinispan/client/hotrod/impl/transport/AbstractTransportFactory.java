package org.infinispan.client.hotrod.impl.transport;

import org.infinispan.client.hotrod.impl.TransportFactory;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;

/**
 * // TODO: Document this
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public abstract class AbstractTransportFactory implements TransportFactory {

   private static Log log = LogFactory.getLog(AbstractTransportFactory.class);

   protected Set<InetSocketAddress> serverAddresses = new HashSet<InetSocketAddress>();

   public void init(Properties props) {
      String servers = props.getProperty(CONF_HOTROD_SERVERS);
      if (servers == null) {
         servers = System.getProperty(OVERRIDE_HOTROD_SERVERS);
         if (servers != null) {
            log.info("Overwriting default server properties (-D" + OVERRIDE_HOTROD_SERVERS + ") with " + servers);
         } else {
            servers = "127.0.0.1:11311";
         }
         log.info("'hotrod-servers' property not specified in config, using " + servers);
      }
      StringTokenizer tokenizer = new StringTokenizer(servers, ";");
      while (tokenizer.hasMoreTokens()) {
         String server = tokenizer.nextToken();
         String[] serverDef = tokenizeServer(server);
         String serverHost = serverDef[0];
         int serverPort = Integer.parseInt(serverDef[1]);
         serverAddresses.add(new InetSocketAddress(serverHost, serverPort));
      }
      if (serverAddresses.isEmpty()) {
         throw new IllegalStateException("No hot-rod servers specified!");
      }
   }

   private String[] tokenizeServer(String server) {
      StringTokenizer t = new StringTokenizer(server, ":");
      return new String[]{t.nextToken(), t.nextToken()};
   }
}
