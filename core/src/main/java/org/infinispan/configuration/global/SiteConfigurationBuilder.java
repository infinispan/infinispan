package org.infinispan.configuration.global;

/**
 * @author Mircea.Markus@jboss.com
 * @since 5.2
 */
public class SiteConfigurationBuilder extends AbstractGlobalConfigurationBuilder<SiteConfiguration> {

   private String localSite;

   SiteConfigurationBuilder(GlobalConfigurationBuilder globalConfig) {
      super(globalConfig);
   }

   /**
    * Sets the name of the local site. Must be a valid name from the list of sites defined.
    */
   public SiteConfigurationBuilder localSite(String localSite) {
      this.localSite = localSite;
      return this;
   }

   @Override
   void validate() {
   }

   @Override
   SiteConfiguration create() {
      return new SiteConfiguration(localSite);
   }

   @Override
   protected GlobalConfigurationChildBuilder read(SiteConfiguration template) {
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
