package org.infinispan.client.hotrod.impl.transport;

import org.infinispan.client.hotrod.impl.TransportFactory;

import java.util.Properties;
import java.util.StringTokenizer;

/**
 * // TODO: Document this
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public abstract class AbstractTransportFactory implements TransportFactory {
   protected String serverHost;
   protected int serverPort;

   public void init(Properties props) {
      String servers = props.getProperty("hotrod-servers");
      StringTokenizer tokenizer = new StringTokenizer(servers,";");
      String server = tokenizer.nextToken();
      String[] serverDef = tokenizeServer(server);
      serverHost = serverDef[0];
      serverPort = Integer.parseInt(serverDef[1]);
   }

   private String[] tokenizeServer(String server) {
      StringTokenizer t = new StringTokenizer(server, ":");
      return new String[] {t.nextToken(), t.nextToken()};
   }
}
