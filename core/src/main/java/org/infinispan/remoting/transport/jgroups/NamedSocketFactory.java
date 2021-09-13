package org.infinispan.remoting.transport.jgroups;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import javax.net.ServerSocketFactory;

import org.jgroups.util.SocketFactory;
import org.jgroups.util.Util;

/**
 * A {@link SocketFactory} which allows setting a callback to configure the sockets using a supplied name.
 *
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 13.0
 **/
public class NamedSocketFactory implements SocketFactory {
   private final Supplier<javax.net.SocketFactory> socketFactory;
   private final Supplier<ServerSocketFactory> serverSocketFactory;
   private String name;
   private BiConsumer<String, Socket> socketConfigurator = (c, s) -> {};
   private BiConsumer<String, ServerSocket> serverSocketConfigurator = (c, s) -> {};

   public NamedSocketFactory(Supplier<javax.net.SocketFactory> socketFactory, Supplier<ServerSocketFactory> serverSocketFactory) {
      this.socketFactory = socketFactory;
      this.serverSocketFactory = serverSocketFactory;
   }

   NamedSocketFactory(NamedSocketFactory original, String name) {
      this.socketFactory = original.socketFactory;
      this.serverSocketFactory = original.serverSocketFactory;
      this.socketConfigurator = original.socketConfigurator;
      this.serverSocketConfigurator = original.serverSocketConfigurator;
      this.name = name;
   }

   public String getName() {
      return name;
   }

   public void setName(String name) {
      this.name = name;
   }

   private Socket configureSocket(Socket socket) {
      socketConfigurator.accept(name, socket);
      return socket;
   }

   private ServerSocket configureSocket(ServerSocket socket) {
      serverSocketConfigurator.accept(name, socket);
      return socket;
   }

   @Override
   public Socket createSocket(String s) throws IOException {
      return configureSocket(socketFactory.get().createSocket());
   }

   @Override
   public Socket createSocket(String s, String host, int port) throws IOException {
      return configureSocket(socketFactory.get().createSocket(host, port));
   }

   @Override
   public Socket createSocket(String s, InetAddress host, int port) throws IOException {
      return configureSocket(socketFactory.get().createSocket(host, port));
   }

   @Override
   public Socket createSocket(String s, String host, int port, InetAddress localHost, int localPort) throws IOException {
      return configureSocket(socketFactory.get().createSocket(host, port, localHost, localPort));
   }

   @Override
   public Socket createSocket(String s, InetAddress host, int port, InetAddress localHost, int localPort) throws IOException {
      return configureSocket(socketFactory.get().createSocket(host, port, localHost, localPort));
   }

   @Override
   public ServerSocket createServerSocket(String s) throws IOException {
      return configureSocket(serverSocketFactory.get().createServerSocket());
   }

   @Override
   public ServerSocket createServerSocket(String s, int port) throws IOException {
      return configureSocket(serverSocketFactory.get().createServerSocket(port));
   }

   @Override
   public ServerSocket createServerSocket(String s, int port, int backlog) throws IOException {
      return configureSocket(serverSocketFactory.get().createServerSocket(port, backlog));
   }

   @Override
   public ServerSocket createServerSocket(String s, int port, int backlog, InetAddress bindAddress) throws IOException {
      return configureSocket(serverSocketFactory.get().createServerSocket(port, backlog, bindAddress));
   }

   public DatagramSocket createDatagramSocket(String service_name) throws SocketException {
      return new DatagramSocket();
   }

   public DatagramSocket createDatagramSocket(String service_name, SocketAddress bindaddr) throws SocketException {
      return new DatagramSocket(bindaddr);
   }

   public DatagramSocket createDatagramSocket(String service_name, int port) throws SocketException {
      return new DatagramSocket(port);
   }

   public DatagramSocket createDatagramSocket(String service_name, int port, InetAddress laddr) throws SocketException {
      return new DatagramSocket(port, laddr);
   }

   public MulticastSocket createMulticastSocket(String service_name) throws IOException {
      return new MulticastSocket();
   }

   public MulticastSocket createMulticastSocket(String service_name, int port) throws IOException {
      return new MulticastSocket(port);
   }

   public MulticastSocket createMulticastSocket(String service_name, SocketAddress bindaddr) throws IOException {
      return new MulticastSocket(bindaddr);
   }

   @Override
   public void close(Socket socket) throws IOException {
      Util.close(socket);
   }

   @Override
   public void close(ServerSocket serverSocket) throws IOException {
      Util.close(serverSocket);
   }

   @Override
   public void close(DatagramSocket datagramSocket) {
      Util.close(datagramSocket);
   }

   @Override
   public Map<Object, String> getSockets() {
      return null;
   }
}
