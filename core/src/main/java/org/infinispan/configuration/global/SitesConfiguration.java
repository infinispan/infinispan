/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */

package org.infinispan.configuration.global;

import java.util.List;

/**
 * @author Mircea.Markus@jboss.com
 * @since 5.2
 */
public class SitesConfiguration {
   private final String localSite;
   private final List<SiteConfiguration> siteConfigurations;

   SitesConfiguration(String localSite, List<SiteConfiguration> siteConfigurations) {
      this.localSite = localSite;
      this.siteConfigurations = siteConfigurations;
   }

   /**
    * Returns the name of the local site. Must be a valid name defined in {@link #siteConfigurations()}
    */
   public final String localSite() {
      return localSite;
   }

   /**
    * Returns a list of all the sites where caches can backup data.
    */
   public final List<SiteConfiguration> siteConfigurations() {
      return siteConfigurations;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof SitesConfiguration)) return false;

      SitesConfiguration that = (SitesConfiguration) o;

      if (localSite != null ? !localSite.equals(that.localSite) : that.localSite != null) return false;
      if (siteConfigurations != null ? !siteConfigurations.equals(that.siteConfigurations) : that.siteConfigurations != null)
         return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = localSite != null ? localSite.hashCode() : 0;
      result = 31 * result + (siteConfigurations != null ? siteConfigurations.hashCode() : 0);
      return result;
   }

   @Override
   public String toString() {
      return "SitesConfiguration{" +
            "localSite='" + localSite + '\'' +
            ", siteConfigurations=" + siteConfigurations +
            '}';
   }
}
