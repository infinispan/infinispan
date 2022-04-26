package org.infinispan.server.hotrod;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.net.InetAddress;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Set;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.marshall.AbstractExternalizer;

/**
 * A Hot Rod server address which encapsulates a multi-homed server. This class enumerates all available addresses on
 * all of the local interfaces.
 *
 * @author Tristan Tarrant
 * @author Galder Zamarreño
 * @since 10.1
 */
public class MultiHomedServerAddress implements ServerAddress {
   private final int port;
   private final List<InetAddressWithNetMask> addresses;

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

   private MultiHomedServerAddress(List<InetAddressWithNetMask> addresses, int port) {
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

   public static class InetAddressWithNetMask {
      final InetAddress address;
      final short prefixLength;

      public InetAddressWithNetMask(InetAddress address, short prefixLength, boolean networkPrefixOverride) {
         this.address = address;
         if (networkPrefixOverride) {
            byte[] a = address.getAddress();
            if (a.length == 4) { // IPv4
               if (a[0] == 10) {
                  this.prefixLength = 8;
               } else if (a[0] == (byte) 192 && a[1] == (byte) 168) {
                  this.prefixLength = 16;
               } else if (a[0] == (byte) 172 && a[1] >= 16 && a[1] <= 31) {
                  this.prefixLength = 12;
               } else if (a[0] == (byte) 169 && a[1] == (byte) 254) {
                  this.prefixLength = 16;
               } else if (a[0] >= (byte) 240 && a[0] <= (byte) 255) {
                  this.prefixLength = 4;
               } else {
                  this.prefixLength = prefixLength;
               }
            } else { // IPv6
               if (a[0] == (byte) 0xfd && a[1] == 0) {
                  this.prefixLength = 8;
               } else if (a[0] == (byte) 0xfc && a[1] == 0) {
                  this.prefixLength = 7;
               } else if (a[0] == (byte) 0xfe && a[1] == (byte) 80) {
                  this.prefixLength = 10;
               } else {
                  this.prefixLength = prefixLength;
               }
            }
         } else {
            this.prefixLength = prefixLength;
         }
      }

      public InetAddressWithNetMask(InetAddress address, short prefixLength) {
         this.address = address;
         this.prefixLength = prefixLength;
      }

      @Override
      public String toString() {
         return address + "/" + prefixLength;
      }
   }

   static class Externalizer extends AbstractExternalizer<MultiHomedServerAddress> {
      @Override
      public Set<Class<? extends MultiHomedServerAddress>> getTypeClasses() {
         return Collections.singleton(MultiHomedServerAddress.class);
      }

      @Override
      public void writeObject(ObjectOutput output, MultiHomedServerAddress object) throws IOException {
         output.writeInt(object.addresses.size());
         for (InetAddressWithNetMask address : object.addresses) {
            output.writeObject(address.address.getHostAddress());
            output.writeShort(address.prefixLength);
         }
         output.writeShort(object.port);
      }

      @Override
      public MultiHomedServerAddress readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         int size = input.readInt();
         List<InetAddressWithNetMask> addresses = new ArrayList<>(size);
         for (int i = 0; i < size; i++) {
            String address = (String) input.readObject();
            short prefixLength = input.readShort();
            addresses.add(new InetAddressWithNetMask(InetAddress.getByName(address), prefixLength));
         }
         int port = input.readUnsignedShort();
         return new MultiHomedServerAddress(addresses, port);
      }
   }
}
