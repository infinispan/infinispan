package org.infinispan.server.test.util;

import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;

/**
 * Client for communication with JGroups protocol in server.
 *
 * @author Jiri Holusa (jholusa@redhat.com)
 */
public class JGroupsProbeClient {

   private String address = System.getProperty("partition.handling.diagnostics.address","224.0.75.75");
   private int port = Integer.parseInt(System.getProperty("partition.handling.diagnostics.port", "7500"));

   private static final int SOCKET_TIMEOUT = 20000;

   protected static Log log = LogFactory.getLog(JGroupsProbeClient.class);

   public JGroupsProbeClient(String diagnosticsAddress, int diagnosticsPort) {
      address = diagnosticsAddress;
      port = diagnosticsPort;
   }

   /**
    * Sends JGroups Probe query to defined address.
    *
    * @param query
    */
   public void send(String query) {
      MulticastSocket socket = null;

      byte[] payload = query.getBytes();

      try {
         socket = new MulticastSocket();
         socket.setSoTimeout(SOCKET_TIMEOUT);
         InetAddress targetAddress = InetAddress.getByName(address);
         DatagramPacket packet = new DatagramPacket(payload, 0, payload.length, targetAddress, port);
         socket.send(packet);

         long start = System.currentTimeMillis();

         if (start + SOCKET_TIMEOUT < System.currentTimeMillis()) {
            String errorMessage = "Timed out waiting for query responses";
            log.error(errorMessage);
            throw new IllegalStateException(errorMessage);
         }

         byte[] buffer = new byte[1024 * 64];
         DatagramPacket result = new DatagramPacket(buffer, buffer.length);
         socket.receive(result);

         log.info("Received response %s from %s:%d", new String(result.getData(), 0, result.getLength()), result.getAddress(), result.getPort());
      } catch (IOException e) {
         String errorMessage = "Exception while performing multicast socket operation. Make sure 'enable_diagnostics' property of TP is enabled " +
               "and correct diagnostics port is specified.";
         log.error(errorMessage);
         throw new IllegalStateException(errorMessage, e);
      } finally {
         if (socket != null) {
            socket.close();
         }
      }
   }

}
