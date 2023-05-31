package org.infinispan.configuration.global;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.remoting.transport.Transport;

/**
 * @author Mircea.Markus@jboss.com
 * @since 5.2
 * @deprecated since 11.0. The local site name is fetched via {@link Transport#localSiteName()}
 */
@Deprecated
public class SiteConfigurationBuilder extends AbstractGlobalConfigurationBuilder implements Builder<SiteConfiguration> {

   private String localSite;

   SiteConfigurationBuilder(GlobalConfigurationBuilder globalConfig) {
      super(globalConfig);
   }

   @Override
   public AttributeSet attributes() {
      return AttributeSet.EMPTY;
   }

   /**
    * Sets the name of the local site. Must be a valid name from the list of sites defined.
    */
   @Deprecated
   public SiteConfigurationBuilder localSite(String localSite) {
      this.localSite = localSite;
      return this;
   }

   @Override
   public
   void validate() {
   }

   @Override
   public
   SiteConfiguration create() {
      return new SiteConfiguration(localSite);
   }

   @Override
   public SiteConfigurationBuilder read(SiteConfiguration template, Combine combine) {
      this.localSite = template.localSite();
      return this;
   }

   @Override
   public String toString() {
      return "SiteConfigurationBuilder{" +
            "localSite='" + localSite + '\'' +
            '}';
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof SiteConfigurationBuilder)) return false;

      SiteConfigurationBuilder that = (SiteConfigurationBuilder) o;

      if (localSite != null ? !localSite.equals(that.localSite) : that.localSite != null) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = localSite != null ? localSite.hashCode() : 0;
      return result;
   }
}
