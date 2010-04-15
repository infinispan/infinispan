package org.infinispan.client.hotrod.impl.transport.tcp;

import org.apache.commons.pool.BaseKeyedPoolableObjectFactory;
import org.apache.commons.pool.BasePoolableObjectFactory;
import org.apache.commons.pool.KeyedPoolableObjectFactory;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.SocketChannel;

/**
 * // TODO: Document this
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public class TcpConnectionFactory extends BaseKeyedPoolableObjectFactory {

   private static Log log = LogFactory.getLog(TcpConnectionFactory.class);

   @Override
   public Object makeObject(Object key) throws Exception {
      InetSocketAddress serverAddress = (InetSocketAddress) key;
      if (log.isTraceEnabled()) {
         log.trace("Creating connection to server: " + serverAddress);
      }
      try {
         SocketChannel socketChannel = SocketChannel.open(serverAddress);
         return socketChannel.socket();
      } catch (IOException e) {
         log.warn("Could not create connection to " + serverAddress, e);
         throw e;
      }
   }

   @Override
   public boolean validateObject(Object key, Object obj) {
      Socket socket = (Socket) obj;
      if (log.isTraceEnabled()) {
         log.trace("About to validate(ping) connection to server " + key + ". socket is " + socket);
      }
      //todo implement
      return true;
   }

   @Override
   public void destroyObject(Object key, Object obj) throws Exception {
      Socket socket = (Socket) obj;
      if (log.isTraceEnabled()) {
         log.trace("About to destroy socket " + socket);
      }
      try {
         socket.close();
      } catch (IOException e) {
         log.warn("Issues closing the socket: " + socket, e);
      }
   }
}
