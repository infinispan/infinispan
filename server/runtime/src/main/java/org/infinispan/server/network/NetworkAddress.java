package org.infinispan.server.network;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InterfaceAddress;
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
   static final int[] NETMASK_BY_PREFIX = { 0,  128,  192,  224,  240,  248,  252,  254,  255};

   private final String name;
   private final InetAddress address;
   private final short prefixLength;

   NetworkAddress(String name, InterfaceAddress address) {
      this.name = name;
      this.address = address.getAddress();
      this.prefixLength = address.getNetworkPrefixLength();

   }

   NetworkAddress(String name, InetAddress address) {
      this.name = name;
      this.address = address;
      this.prefixLength = 0;
   }

   /**
    * The name of this address
    */
   public String getName() {
      return name;
   }

   /**
    * The IP address
    */
   public InetAddress getAddress() {
      return address;
   }

   public String cidr() {
      StringBuilder sb = new StringBuilder();
      byte[] bytes = address.getAddress();
      short prefix = prefixLength;

      for (int i = 0; i < bytes.length; i++) {
         if (i > 0) {
            sb.append('.');
         }
         int bits = prefix > 8 ? 8 : prefix;
         sb.append((int)bytes[i] & NETMASK_BY_PREFIX[bits]);
         prefix -= bits;
      }
      sb.append('/').append(prefixLength);
      return sb.toString();
   }

   @Override
   public String toString() {
      return "NetworkAddress{" +
            "name='" + name + '\'' +
            ", address=" + address +
            ", prefixLength=" + prefixLength +
            '}';
   }

   public short getPrefixLength() {
      return prefixLength;
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
      return new NetworkAddress(name, findInterface(i -> i.getName().matches(regex)).getInterfaceAddresses().get(0));
   }

   public static NetworkAddress matchAddress(String name, String regex) throws IOException {
      return new NetworkAddress(name, findAddress(a -> a.getHostAddress().matches(regex)));
   }

   public static NetworkAddress matchHost(String name, String regex) throws IOException {
      return new NetworkAddress(name, findAddress(a -> a.getHostName().matches(regex)));
   }

   public static NetworkAddress match(String name, Predicate<NetworkInterface> ifMatcher, Predicate<InetAddress> addressMatcher) throws IOException {
      NetworkInterface networkInterface = findInterface(ifMatcher);
      InterfaceAddress address = findAddress(networkInterface, addressMatcher);
      if (address != null) {
         return new NetworkAddress(name, address);
      } else {
         throw new IOException("No matching addresses found");
      }
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

   private static InterfaceAddress findAddress(Predicate<InetAddress> matcher) throws IOException {
      for (Enumeration<java.net.NetworkInterface> interfaces = java.net.NetworkInterface.getNetworkInterfaces(); interfaces.hasMoreElements(); ) {
         NetworkInterface networkInterface = interfaces.nextElement();
         if (networkInterface.isUp()) {
            if (Server.log.isDebugEnabled()) {
               Server.log.debugf("Network interface %s", networkInterface);
            }
            InterfaceAddress ifAddress = findAddress(networkInterface, matcher);
            if (ifAddress != null) return ifAddress;
         }
      }
      throw new IOException("No matching addresses found");
   }

   private static InterfaceAddress findAddress(NetworkInterface networkInterface, Predicate<InetAddress> matcher) throws IOException {
      for (InterfaceAddress ifAddress : networkInterface.getInterfaceAddresses()) {
         InetAddress address = ifAddress.getAddress();
         if (Server.log.isDebugEnabled()) {
            Server.log.debugf("Network address %s (loopback=%b linklocal=%b sitelocal=%b)", address, address.isLoopbackAddress(), address.isLinkLocalAddress(), address.isSiteLocalAddress());
         }
         if (matcher.test(address)) {
            return ifAddress;
         }
      }
      return null;
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
