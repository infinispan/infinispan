package org.infinispan.client.hotrod.impl.transport.tcp;

import org.infinispan.client.hotrod.impl.Transport;
import org.infinispan.client.hotrod.impl.TransportFactory;

import java.util.Properties;
import java.util.StringTokenizer;

/**
 * // TODO: Document this
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public class TcpTransportFactory implements TransportFactory {

   private String serverHost;
   private int serverPort;

   public void init(Properties props) {
      String servers = props.getProperty("hotrod-servers");
      StringTokenizer tokenizer = new StringTokenizer(servers,";");
      String server = tokenizer.nextToken();
      String[] serverDef = tokenizeServer(server);
      serverHost = serverDef[0];
      serverPort = Integer.parseInt(serverDef[1]);
   }

   @Override
   public void destroy() {
      // TODO: Customise this generated block
   }

   private String[] tokenizeServer(String server) {
      StringTokenizer t = new StringTokenizer(server, ":");
      return new String[] {t.nextToken(), t.nextToken()};
   }

   @Override
   public Transport getTransport() {
      TcpTransport transport = new TcpTransport(serverHost, serverPort);
      transport.connect();
      return transport;
   }
}
