/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later.
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.infinispan.hibernate.cache.v53.impl;

import org.hibernate.cache.CacheException;
import org.hibernate.cache.spi.DomainDataRegion;
import org.hibernate.cache.spi.access.AccessType;
import org.hibernate.cache.spi.access.SoftLock;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.infinispan.hibernate.cache.commons.access.AccessDelegate;

/**
 * Collection region access for Infinispan.
 *
 * @author Chris Bredesen
 * @author Galder Zamarreño
 * @since 3.5
 */
abstract class AbstractAccess {
   private final AccessType accessType;
   final AccessDelegate delegate;
   final DomainDataRegionImpl region;

   AbstractAccess(AccessType accessType, AccessDelegate delegate, DomainDataRegionImpl region) {
      this.accessType = accessType;
      this.delegate = delegate;
      this.region = region;
   }

   public void evict(Object key) throws CacheException {
      delegate.evict( key );
   }

   public void evictAll() throws CacheException {
      delegate.evictAll();
   }

   public void removeAll(SharedSessionContractImplementor session) throws CacheException {
      delegate.removeAll();
   }

   public SoftLock lockRegion() throws CacheException {
      return null;
   }

   public void unlockRegion(SoftLock lock) throws CacheException {
   }

   public AccessType getAccessType() {
      return accessType;
   }

   public DomainDataRegion getRegion() {
      return region;
   }

   public boolean contains(Object key) {
      return delegate.get(null, key, Long.MAX_VALUE) != null;
   }
}
