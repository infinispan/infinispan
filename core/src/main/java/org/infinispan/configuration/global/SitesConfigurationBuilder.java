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

import org.infinispan.config.ConfigurationException;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Mircea.Markus@jboss.com
 * @since 5.2
 */
public class SitesConfigurationBuilder extends AbstractGlobalConfigurationBuilder<SitesConfiguration> {

   private String localSite;

   SitesConfigurationBuilder(GlobalConfigurationBuilder globalConfig) {
      super(globalConfig);
   }

   /**
    * Sets the name of the local site. Must be a valid name from the list of sites defined.
    */
   public SitesConfigurationBuilder localSite(String localSite) {
      this.localSite = localSite;
      return this;
   }

   @Override
   void validate() {
   }

   @Override
   SitesConfiguration create() {
      return new SitesConfiguration(localSite);
   }

   @Override
   protected GlobalConfigurationChildBuilder read(SitesConfiguration template) {
      this.localSite = template.localSite();
      return this;
   }

   @Override
   public String toString() {
      return "SitesConfigurationBuilder{" +
            "localSite='" + localSite + '\'' +
            '}';
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof SitesConfigurationBuilder)) return false;

      SitesConfigurationBuilder that = (SitesConfigurationBuilder) o;

      if (localSite != null ? !localSite.equals(that.localSite) : that.localSite != null) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = localSite != null ? localSite.hashCode() : 0;
      return result;
   }
}
