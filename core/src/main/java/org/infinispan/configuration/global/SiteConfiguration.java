package org.infinispan.configuration.global;

/**
 * @author Mircea.Markus@jboss.com
 * @since 5.2
 */
public class SiteConfiguration {
   private final String localSite;

   SiteConfiguration(String localSite) {
      this.localSite = localSite;
   }

   /**
    * Returns the name of the local site. Must be a valid name defined in {@link #siteConfigurations()}
    */
   public final String localSite() {
      return localSite;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof SiteConfiguration)) return false;

      SiteConfiguration that = (SiteConfiguration) o;

      if (localSite != null ? !localSite.equals(that.localSite) : that.localSite != null) return false;

      return true;
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
