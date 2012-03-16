/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.loaders.cluster;

import org.infinispan.loaders.AbstractCacheLoaderConfig;

/**
 * Configuration for {@link org.infinispan.loaders.cluster.ClusterCacheLoader}.
 *
 * @author Mircea.Markus@jboss.com
 */
public class ClusterCacheLoaderConfig extends AbstractCacheLoaderConfig {

   private static final long serialVersionUID = -44748358960849539L;
   
   private long remoteCallTimeout;

   public String getCacheLoaderClassName() {
      return ClusterCacheLoader.class.getName();
   }

   public long getRemoteCallTimeout() {
      return remoteCallTimeout;
   }

   /**
    * @deprecated The visibility of this will be reduced, {@link #remoteCallTimeout(long)}
    */
   @Deprecated
   public void setRemoteCallTimeout(long remoteCallTimeout) {
      testImmutability("remoteCallTimeout");
      this.remoteCallTimeout = remoteCallTimeout;
   }

   public ClusterCacheLoaderConfig remoteCallTimeout(long remoteCallTimeout) {
      setRemoteCallTimeout(remoteCallTimeout);
      return this;
   }
}
