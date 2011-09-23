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
package org.infinispan.cdi.test.tck;

import javax.cache.CacheManager;
import javax.cache.OptionalFeature;
import javax.cache.spi.CacheManagerFactoryProvider;

/**
 * This implementation is only here for test purpose. To pass the JCache TCK for the annotations part we have to say
 * that this optional feature is supported by our implementation.
 *
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 */
public class CacheManagerFactoryProviderImpl implements CacheManagerFactoryProvider {

   @Override
   public CacheManager createCacheManager(String name) {
      return null;
   }

   @Override
   public boolean isSupported(OptionalFeature optionalFeature) {
      return optionalFeature == OptionalFeature.ANNOTATIONS;
   }
}
