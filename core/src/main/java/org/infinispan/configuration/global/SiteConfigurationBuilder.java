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
