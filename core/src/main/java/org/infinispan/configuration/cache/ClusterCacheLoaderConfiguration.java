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

import org.infinispan.config.parsing.XmlConfigHelper;
import org.infinispan.configuration.BuiltBy;
import org.infinispan.loaders.cluster.ClusterCacheLoaderConfig;
import org.infinispan.util.TypedProperties;

/**
 * ClusterCacheLoaderConfiguration.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
@BuiltBy(ClusterCacheLoaderConfigurationBuilder.class)
public class ClusterCacheLoaderConfiguration extends AbstractLoaderConfiguration implements LegacyLoaderAdapter<ClusterCacheLoaderConfig> {
   private final long remoteCallTimeout;

   ClusterCacheLoaderConfiguration(long remoteCallTimeout, TypedProperties properties) {
      super(properties);
      this.remoteCallTimeout = remoteCallTimeout;
   }

   public long remoteCallTimeout() {
      return remoteCallTimeout;
   }

   @Override
   public String toString() {
      return "ClusterCacheLoaderConfiguration [remoteCallTimeout=" + remoteCallTimeout + "]";
   }

   @Override
   public ClusterCacheLoaderConfig adapt() {
      ClusterCacheLoaderConfig config = new ClusterCacheLoaderConfig();
      config.remoteCallTimeout(remoteCallTimeout);
      XmlConfigHelper.setValues(config, properties(), false, true);
      return config;
   }


}
