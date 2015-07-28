package org.infinispan.test.integration.security.utils;

/**
 *
 * @author <a href="mailto:vchepeli@redhat.com">Vitalii Chepeliuk</a>
 * @since 8.0
 */
public class ManagementClientParams {

   private String hostname;
   private Integer port;

   public ManagementClientParams(String hostname, Integer port) {
      this.hostname = hostname;
      this.port = port;
   }

   public String getHostname() {
      return hostname;
   }

   public Integer getPort() {
      return port;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof ManagementClientParams)) return false;

      ManagementClientParams params = (ManagementClientParams) o;

      if (!hostname.equals(params.hostname)) return false;
      return port.equals(params.port);

   }

   @Override
   public int hashCode() {
      int result = hostname.hashCode();
      result = 31 * result + port.hashCode();
      return result;
   }
}
