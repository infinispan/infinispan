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
import org.infinispan.remoting.transport.Address;

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
   public MultiHomedServerAddress(int port) {
      this.port = port;
      addresses = new ArrayList<>();
      try {
         for (Enumeration<NetworkInterface> en = NetworkInterface.getNetworkInterfaces(); en.hasMoreElements(); ) {
            NetworkInterface intf = en.nextElement();
            for (InterfaceAddress address : intf.getInterfaceAddresses()) {
               addresses.add(new InetAddressWithNetMask(address));
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
   public int compareTo(Address o) {
      if (o instanceof MultiHomedServerAddress) {
         MultiHomedServerAddress oa = (MultiHomedServerAddress) o;
         int cmp = addresses.size() - oa.addresses.size();
         if (cmp == 0) {
            cmp = port - oa.port;
         }
         return cmp;
      }
      return -1;
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
            return address.address.getHostAddress();
         }
      }
      throw new IllegalArgumentException("No interface address matching " + localAddress);
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
            if ((a & netMaskByPrefix[prefixLength - 1]) != (b & netMaskByPrefix[prefixLength - 1])) {
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

      public InetAddressWithNetMask(InterfaceAddress address) {
         this.address = address.getAddress();
         this.prefixLength = address.getNetworkPrefixLength();
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
