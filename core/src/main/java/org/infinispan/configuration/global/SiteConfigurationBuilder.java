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

/**
 * @author Mircea.Markus@jboss.com
 * @since 5.2
 */
public class SiteConfigurationBuilder extends AbstractGlobalConfigurationBuilder<SiteConfiguration> {

   private String name;

   SiteConfigurationBuilder(GlobalConfigurationBuilder globalConfig) {
      super(globalConfig);
   }

   /**
    * Sets site's name. Must be defined and it must match the site names as defined at transport(jgroups)
    * level.
    */
   public SiteConfigurationBuilder name(String name) {
      this.name = name;
      return this;
   }

   @Override
   void validate() {
      if (name == null)
         throw new ConfigurationException("Name is a required property for the site configuration.");
   }

   @Override
   SiteConfiguration create() {
      return new SiteConfiguration(name);
   }

   @Override
   protected SiteConfigurationBuilder read(SiteConfiguration template) {
      name = template.name();
      return this;
   }

   @Override
   public String toString() {
      return "SiteConfigurationBuilder{" +
            "name='" + name + '\'' +
            '}';
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof SiteConfigurationBuilder)) return false;

      SiteConfigurationBuilder that = (SiteConfigurationBuilder) o;

      if (name != null ? !name.equals(that.name) : that.name != null) return false;

      return true;
   }

   @Override
   public int hashCode() {
      return name != null ? name.hashCode() : 0;
   }

   boolean isSameName(String localSite) {
      return localSite.equals(name);
   }
}
