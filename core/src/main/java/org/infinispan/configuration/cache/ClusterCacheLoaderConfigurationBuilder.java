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
package org.infinispan.configuration.cache;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.infinispan.config.parsing.XmlConfigHelper;
import org.infinispan.util.TypedProperties;

public class ClusterCacheLoaderConfigurationBuilder extends AbstractLoaderConfigurationBuilder<ClusterCacheLoaderConfiguration, ClusterCacheLoaderConfigurationBuilder> {
   private long remoteCallTimeout;

   protected ClusterCacheLoaderConfigurationBuilder(LoadersConfigurationBuilder builder) {
      super(builder);
   }

   @Override
   public ClusterCacheLoaderConfigurationBuilder self() {
      return this;
   }

   public ClusterCacheLoaderConfigurationBuilder remoteCallTimeout(long remoteCallTimeout) {
      this.remoteCallTimeout = remoteCallTimeout;
      return this;
   }

   public ClusterCacheLoaderConfigurationBuilder remoteCallTimeout(long remoteCallTimeout, TimeUnit unit) {
      this.remoteCallTimeout = unit.toMillis(remoteCallTimeout);
      return this;
   }

   @Override
   public ClusterCacheLoaderConfigurationBuilder withProperties(Properties p) {
      this.properties = p;
      // TODO: Remove this and any sign of properties when switching to new cache store configs
      XmlConfigHelper.setValues(this, properties, false, true);
      return this;
   }

   @Override
   public void validate() {
   }

   @Override
   public ClusterCacheLoaderConfiguration create() {
      return new ClusterCacheLoaderConfiguration(remoteCallTimeout, TypedProperties.toTypedProperties(properties));
   }

   @Override
   public ClusterCacheLoaderConfigurationBuilder read(ClusterCacheLoaderConfiguration template) {
      this.remoteCallTimeout = template.remoteCallTimeout();
      this.properties = template.properties();
      return this;
   }
}
