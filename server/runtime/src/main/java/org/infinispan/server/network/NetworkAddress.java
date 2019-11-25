package org.infinispan.server.network;

import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.UnknownHostException;
import java.util.Enumeration;
import java.util.function.Predicate;

import org.infinispan.server.Server;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class NetworkAddress {
   private final String name;
   private final InetAddress address;

   NetworkAddress(String name, InetAddress address) {
      this.name = name;
      this.address = address;
   }

   /**
    * The name of this interface
    */
   public String getName() {
      return name;
   }

   /**
    * The network address of this interface
    */
   public InetAddress getAddress() {
      return address;
   }

   @Override
   public String toString() {
      return "NetworkInterfaceImpl{" +
            "name='" + name + '\'' +
            ", address=" + address +
            '}';
   }

   public static NetworkAddress fromString(String name, String value) throws IOException {
      switch (value) {
         case "GLOBAL":
            return globalAddress(name);
         case "LOOPBACK":
            return loopback(name);
         case "NON_LOOPBACK":
            return nonLoopback(name);
         case "SITE_LOCAL":
            return siteLocal(name);
         case "LINK_LOCAL":
            return linkLocalAddress(name);
         default:
            if (value.startsWith("match-interface:")) {
               return matchInterface(name, value.substring(value.indexOf(':') + 1));
            } else if (value.startsWith("match-address:")) {
               return matchAddress(name, value.substring(value.indexOf(':') + 1));
            } else if (value.startsWith("match-host:")) {
               return matchHost(name, value.substring(value.indexOf(':') + 1));
            } else {
               return inetAddress(name, value);
            }
      }
   }

   public static NetworkAddress globalAddress(String name) throws IOException {
      return new NetworkAddress(name, findAddress(a -> !a.isLoopbackAddress() && !a.isSiteLocalAddress() && !a.isLinkLocalAddress()));
   }

   public static NetworkAddress loopback(String name) throws IOException {
      return new NetworkAddress(name, findAddress(InetAddress::isLoopbackAddress));
   }

   public static NetworkAddress nonLoopback(String name) throws IOException {
      return new NetworkAddress(name, findAddress(a -> !a.isLoopbackAddress()));
   }

   public static NetworkAddress siteLocal(String name) throws IOException {
      return new NetworkAddress(name, findAddress(InetAddress::isSiteLocalAddress));
   }

   public static NetworkAddress matchInterface(String name, String regex) throws IOException {
      return new NetworkAddress(name, findInterface(i -> i.getName().matches(regex)).getInetAddresses().nextElement());
   }

   public static NetworkAddress matchAddress(String name, String regex) throws IOException {
      return new NetworkAddress(name, findAddress(a -> a.getHostAddress().matches(regex)));
   }

   public static NetworkAddress matchHost(String name, String regex) throws IOException {
      return new NetworkAddress(name, findAddress(a -> a.getHostName().matches(regex)));
   }

   public static NetworkAddress inetAddress(String name, String value) throws UnknownHostException {
      return new NetworkAddress(name, InetAddress.getByName(value));
   }

   public static NetworkAddress anyAddress(String name) throws UnknownHostException {
      return new NetworkAddress(name, InetAddress.getByAddress(new byte[]{0, 0, 0, 0}));
   }

   public static NetworkAddress linkLocalAddress(String name) throws IOException {
      return new NetworkAddress(name, findAddress(InetAddress::isLinkLocalAddress));
   }

   static InetAddress findAddress(Predicate<InetAddress> matcher) throws IOException {
      for (Enumeration<java.net.NetworkInterface> interfaces = java.net.NetworkInterface.getNetworkInterfaces(); interfaces.hasMoreElements(); ) {
         NetworkInterface networkInterface = interfaces.nextElement();
         if (networkInterface.isUp()) {
            if (Server.log.isDebugEnabled()) {
               Server.log.debugf("Network interface %s", networkInterface);
            }
            for (Enumeration addresses = networkInterface.getInetAddresses(); addresses.hasMoreElements(); ) {
               InetAddress address = (InetAddress) addresses.nextElement();
               if (Server.log.isDebugEnabled()) {
                  Server.log.debugf("Network address %s (%b %b %b)", address, address.isLoopbackAddress(), address.isLinkLocalAddress(), address.isSiteLocalAddress());
               }
               if (matcher.test(address)) {
                  return address;
               }
            }
         }
      }
      throw new IOException("No matching addresses found");
   }

   private static NetworkInterface findInterface(Predicate<NetworkInterface> matcher) throws IOException {
      for (Enumeration<java.net.NetworkInterface> interfaces = java.net.NetworkInterface.getNetworkInterfaces(); interfaces.hasMoreElements(); ) {
         NetworkInterface networkInterface = interfaces.nextElement();
         if (networkInterface.isUp()) {
            if (Server.log.isDebugEnabled()) {
               Server.log.debugf("Network interface %s", networkInterface);
            }
            if (matcher.test(networkInterface)) {
               return networkInterface;
            }
         }
      }
      throw new IOException("No matching addresses found");
   }
}
