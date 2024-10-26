package org.infinispan.configuration.global;

import java.util.Objects;

import org.infinispan.remoting.transport.Transport;

/**
 * @author Mircea.Markus@jboss.com
 * @since 5.2
 * @deprecated since 11.0. Use {@link Transport#localSiteName()}
 */
@Deprecated(forRemoval=true, since = "11.0")
public class SiteConfiguration {
   private final String localSite;

   SiteConfiguration(String localSite) {
      this.localSite = localSite;
   }

   /**
    * Returns the name of the local site.
    */
   @Deprecated(forRemoval=true, since = "11.0")
   public final String localSite() {
      return localSite;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof SiteConfiguration that)) return false;
      return Objects.equals(localSite, that.localSite);
   }

   @Override
   public int hashCode() {
      return localSite != null ? localSite.hashCode() : 0;
   }

   @Override
   public String toString() {
      return "SiteConfiguration{" +
            "localSite='" + localSite + '\'' +
            '}';
   }
}
