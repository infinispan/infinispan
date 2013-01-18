/* 
 * JBoss, Home of Professional Open Source 
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tag. All rights reserved. 
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
package org.infinispan.jsr107.cache;

import javax.cache.CacheManagerFactory;
import javax.cache.OptionalFeature;
import javax.cache.spi.CachingProvider;

/**
 * InfinispanCachingProvider is Infinispan's SPI hook up to {@link javax.cache.spi.CachingProvider}.
 * 
 * @author Vladimir Blagojevic
 * @since 5.3
 */
public class InfinispanCachingProvider implements CachingProvider {

   public InfinispanCachingProvider() {
      // required no-op constructor by the spec
   }

   @Override
   public CacheManagerFactory getCacheManagerFactory() {
      return InfinispanCacheManagerFactory.getInstance();
   }

   @Override
   public boolean isSupported(OptionalFeature optionalFeature) {
      switch (optionalFeature) {
         case TRANSACTIONS:
            return true;
         case STORE_BY_REFERENCE:
            return true;
         default:
            return false;
      }
   }

}
