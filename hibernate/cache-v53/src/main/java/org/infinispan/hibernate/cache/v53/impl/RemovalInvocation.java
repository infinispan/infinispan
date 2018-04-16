/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later.
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.infinispan.hibernate.cache.v53.impl;

import java.util.concurrent.CompletableFuture;

import org.infinispan.functional.FunctionalMap;
import org.infinispan.hibernate.cache.commons.InfinispanDataRegion;
import org.infinispan.hibernate.cache.commons.util.VersionedEntry;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class RemovalInvocation implements Invocation {
   private final InfinispanDataRegion region;
   private final Object key;
   private final FunctionalMap.ReadWriteMap<Object, Object> rwMap;

   public RemovalInvocation(FunctionalMap.ReadWriteMap<Object, Object> rwMap, InfinispanDataRegion region, Object key) {
      this.rwMap = rwMap;
      this.region = region;
      this.key = key;
   }

   @Override
   public CompletableFuture<Void> invoke(boolean success) {
      if (success) {
         return rwMap.eval(key, new VersionedEntry(region.nextTimestamp()));
      } else {
         return null;
      }
   }
}
