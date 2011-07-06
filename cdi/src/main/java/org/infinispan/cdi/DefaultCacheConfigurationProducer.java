/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.cdi;

import org.infinispan.config.Configuration;

import javax.enterprise.inject.Default;
import javax.enterprise.inject.Produces;

/**
 * <p>This producer is responsible to produce the default configuration used by the default cache manager.</p>
 * <p>If you want to provide a specific default configuration for the default cache manager follow this steps:
 * <ol>
 *    <li>Extend this bean</li>
 *    <li>Add {@linkplain javax.enterprise.inject.Specializes @Specializes} annotation on your class</li>
 *    <li>Override the {@link DefaultCacheConfigurationProducer#getDefaultCacheConfiguration()} method.</li>
 * </ol></p>
 *
 * @author Pete Muir
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 */
public class DefaultCacheConfigurationProducer {
   /**
    * <p>This producer is responsible to produce the default configuration used by the default cache manager produced
    * by the {@link DefaultCacheManagerProducer}.</p>
    *
    * @return The default configuration used by the default cache manager.
    */
   @Default
   @Infinispan
   @Produces
   public Configuration getDefaultCacheConfiguration() {
      return new Configuration();
   }
}
