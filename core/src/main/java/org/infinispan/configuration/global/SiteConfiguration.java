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
