package org.infinispan.client.hotrod.impl.transport.tcp;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Collection;
import java.util.Properties;

/**
 * // TODO: Document this
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public interface
      TcpConnectionPool {

   public void init(Properties props);

   public void start(Collection<InetSocketAddress> servers);

   public Socket getConnection();

   public void releaseConnection(Socket socket);

   public void updateServers(Collection<InetSocketAddress> newServers);

   public void destroy();
}
