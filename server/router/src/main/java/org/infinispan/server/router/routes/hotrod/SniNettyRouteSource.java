package org.infinispan.server.router.routes.hotrod;

import javax.net.ssl.SSLContext;

import org.infinispan.commons.util.SslContextFactory;
import org.infinispan.server.router.routes.SniRouteSource;

public class SniNettyRouteSource implements SniRouteSource {

   private final SSLContext jdkContext;
   private final String sniHostName;

   public SniNettyRouteSource(String sniHostName, SSLContext sslContext) {
      this.sniHostName = sniHostName;
      this.jdkContext = sslContext;
   }

   public SniNettyRouteSource(String sniHostName, String keyStoreFileName, char[] keyStorePassword) {
      this(sniHostName, new SslContextFactory().keyStoreFileName(keyStoreFileName).keyStorePassword(keyStorePassword).build().sslContext());
   }

   @Override
   public SSLContext getSslContext() {
      return jdkContext;
   }

   @Override
   public String getSniHostName() {
      return sniHostName;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      SniNettyRouteSource that = (SniNettyRouteSource) o;

      return getSniHostName().equals(that.getSniHostName());
   }

   @Override
   public int hashCode() {
      return getSniHostName().hashCode();
   }

   @Override
   public String toString() {
      return "SniNettyRouteSource{" +
            "sniHostName='" + sniHostName + '\'' +
            '}';
   }

   @Override
   public void validate() {
      if (sniHostName == null || sniHostName.isEmpty()) {
         throw new IllegalArgumentException("SNI Host name can not be null");
      }
      if (jdkContext == null) {
         throw new IllegalArgumentException("JDK SSL Context must not be null");
      }
   }
}
