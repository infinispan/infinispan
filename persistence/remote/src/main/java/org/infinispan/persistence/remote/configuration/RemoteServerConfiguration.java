package org.infinispan.persistence.remote.configuration;

public class RemoteServerConfiguration {
   private final String host;
   private final int port;

   RemoteServerConfiguration(String host, int port) {
      this.host = host;
      this.port = port;
   }

   public String host() {
      return host;
   }

   public int port() {
      return port;
   }

   @Override
   public String toString() {
      return "RemoteServerConfiguration{" +
            "host='" + host + '\'' +
            ", port=" + port +
            '}';
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      RemoteServerConfiguration that = (RemoteServerConfiguration) o;

      if (port != that.port) return false;
      return host != null ? host.equals(that.host) : that.host == null;

   }

   @Override
   public int hashCode() {
      int result = host != null ? host.hashCode() : 0;
      result = 31 * result + port;
      return result;
   }
}
