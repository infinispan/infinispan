package org.infinispan.server.hotrod;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.marshall.MarshallingException;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * A Hot Rod server address which encapsulates a multi-homed server. This class enumerates all available addresses on
 * all the local interfaces.
 *
 * @author Tristan Tarrant
 * @author Galder Zamarreño
 * @since 10.1
 */
@ProtoTypeId(ProtoStreamTypeIds.SERVER_HR_MULTI_HOMED_SERVER_ADDRESS)
public class MultiHomedServerAddress implements ServerAddress {

   @ProtoField(1)
   final int port;

   @ProtoField(2)
   final List<InetAddressWithNetMask> addresses;

   /**
    * @param port
    */
   public MultiHomedServerAddress(int port, boolean networkPrefixOverride) {
      this.port = port;
      addresses = new ArrayList<>();
      try {
         for (Enumeration<NetworkInterface> en = NetworkInterface.getNetworkInterfaces(); en.hasMoreElements(); ) {
            NetworkInterface intf = en.nextElement();
            for (InterfaceAddress address : intf.getInterfaceAddresses()) {
               addresses.add(new InetAddressWithNetMask(address.getAddress(), address.getNetworkPrefixLength(), networkPrefixOverride));
            }
         }
      } catch (IOException e) {
         throw new CacheConfigurationException(e);
      }
   }

   @ProtoFactory
   MultiHomedServerAddress(List<InetAddressWithNetMask> addresses, int port) {
      this.addresses = addresses;
      this.port = port;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      MultiHomedServerAddress that = (MultiHomedServerAddress) o;

      if (port != that.port) return false;
      return addresses.equals(that.addresses);

   }

   @Override
   public int hashCode() {
      int result = addresses.hashCode();
      result = 31 * result + port;
      return result;
   }

   @Override
   public String toString() {
      return "MultiHomedServerAddress{" +
            "port=" + port +
            ", addresses=" + addresses +
            '}';
   }

   public int getPort() {
      return port;
   }

   /**
    * Return the interface address which matches the incoming address
    *
    * @param localAddress
    * @return
    */
   @Override
   public String getHost(InetAddress localAddress) {
      for (InetAddressWithNetMask address : addresses) {
         if (inetAddressMatchesInterfaceAddress(localAddress.getAddress(), address.address.getAddress(), address.prefixLength)) {
            if (HotRodServer.log.isDebugEnabled()) {
               HotRodServer.log.debugf("Matched incoming address '%s' with '%s'", localAddress, address);
            }
            return address.address.getHostAddress();
         }
      }
      throw new IllegalArgumentException("No interface address matching '" + localAddress + "' in " + this);
   }

   static byte[] netMaskByPrefix = {(byte) 128, (byte) 192, (byte) 224, (byte) 240, (byte) 248, (byte) 252, (byte) 254};

   /**
    * Checks whether the supplied network address matches the interfaceAddress. It does this by using the interface's
    * prefixLength and comparing the bits in the prefix.
    *
    * @param inetAddress
    * @param interfaceAddress
    * @return
    */
   public static boolean inetAddressMatchesInterfaceAddress(byte[] inetAddress, byte[] interfaceAddress, int prefixLength) {
      if (HotRodServer.log.isDebugEnabled()) {
         HotRodServer.log.debugf("Matching incoming address '%s' with '%s'/%d", inetAddress, interfaceAddress, prefixLength);
      }
      if (inetAddress.length != interfaceAddress.length) {
         return false;
      }
      for (int i = 0; i < inetAddress.length; i++) {
         byte a = inetAddress[i];
         byte b = interfaceAddress[i];
         if (prefixLength >= 8) {
            if (a != b) {
               return false;
            } else {
               prefixLength -= 8;
            }
         } else if (prefixLength > 0) {
            byte mask = MultiHomedServerAddress.netMaskByPrefix[prefixLength - 1];
            if ((a & mask) != (b & mask)) {
               return false;
            } else {
               prefixLength = 0;
            }
         }
      }
      return true;
   }

   @ProtoTypeId(ProtoStreamTypeIds.SERVER_HR_MULTI_HOMED_SERVER_ADDRESS_INET)
   public static class InetAddressWithNetMask {
      // RFC 1918 10.0.0.0/8
      public static final InetAddressWithNetMask RFC1918_CIDR_10 = new InetAddressWithNetMask(new byte[]{10, 0, 0, 0}, (short) 8);
      // RFC 1918 172.16.0.0/12
      public static final InetAddressWithNetMask RFC1918_CIDR_172 = new InetAddressWithNetMask(new byte[]{(byte) 172, (byte) 16, 0, 0}, (short) 12);
      // RFC 1918 192.168.0.0/16
      public static final InetAddressWithNetMask RFC1918_CIDR_192 = new InetAddressWithNetMask(new byte[]{(byte) 192, (byte) 168, 0, 0}, (short) 16);
      // RFC 3927 169.254.0.0/16
      public static final InetAddressWithNetMask RFC3927_LINK_LOCAL = new InetAddressWithNetMask(new byte[]{(byte) 169, (byte) 254, 0, 0}, (short) 16);
      // RFC 1112 240.0.0.0/4
      public static final InetAddressWithNetMask RFC1112_RESERVED = new InetAddressWithNetMask(new byte[]{(byte) 240, 0, 0, 0}, (short) 4);
      // RFC 6598 100.64.0.0/10
      public static final InetAddressWithNetMask RFC6598_SHARED_SPACE = new InetAddressWithNetMask(new byte[]{(byte) 100, (byte) 64, 0, 0}, (short) 10);
      // RFC 4193 fc00::/7
      public static final InetAddressWithNetMask RFC4193_ULA = new InetAddressWithNetMask(new byte[]{(byte) 0xfc, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, (short) 7);
      // RFC 4193 fe80::/10
      public static final InetAddressWithNetMask RFC4193_LINK_LOCAL = new InetAddressWithNetMask(new byte[]{(byte) 0xfe, (byte) 0x80, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, (short) 10);
      public static final List<InetAddressWithNetMask> PRIVATE_NETWORKS = Arrays.asList(
            RFC1918_CIDR_10,
            RFC1918_CIDR_172,
            RFC1918_CIDR_192,
            RFC3927_LINK_LOCAL,
            RFC1112_RESERVED,
            RFC6598_SHARED_SPACE,
            RFC4193_ULA,
            RFC4193_LINK_LOCAL
      );
      final InetAddress address;
      final short prefixLength;

      public InetAddressWithNetMask(InetAddress address, short prefixLength, boolean networkPrefixOverride) {
         this.address = address;
         short prefix = prefixLength;
         if (networkPrefixOverride) {
            byte[] a = address.getAddress();
            for (InetAddressWithNetMask net : PRIVATE_NETWORKS) {
               if (inetAddressMatchesInterfaceAddress(a, net.address.getAddress(), net.prefixLength)) {
                  prefix = net.prefixLength;
                  break;
               }
            }
         }
         this.prefixLength = prefix;
      }

      private InetAddressWithNetMask(byte[] address, short prefixLength) {
         try {
            this.address = InetAddress.getByAddress(address);
         } catch (UnknownHostException e) {
            throw new IllegalArgumentException(e);
         }
         this.prefixLength = prefixLength;
      }

      public InetAddressWithNetMask(InetAddress address, short prefixLength) {
         this.address = address;
         this.prefixLength = prefixLength;
      }

      @ProtoFactory
      static InetAddressWithNetMask protoFactory(String hostAddress, short prefixLength) {
         try {
            return new InetAddressWithNetMask(InetAddress.getByName(hostAddress), prefixLength);
         } catch (UnknownHostException e) {
            throw new MarshallingException(String.format("Unable to resolve InetAddress by name '%s'", hostAddress), e);
         }
      }

      @ProtoField(1)
      String getHostAddress() {
         return address.getHostAddress();
      }

      @ProtoField(2)
      short getPrefixLength() {
         return prefixLength;
      }

      @Override
      public String toString() {
         return address + "/" + prefixLength;
      }
   }
}
