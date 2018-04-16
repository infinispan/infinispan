/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later.
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.infinispan.hibernate.cache.v53.impl;

import java.util.concurrent.CompletableFuture;

import org.infinispan.functional.FunctionalMap;
import org.infinispan.hibernate.cache.commons.util.VersionedEntry;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class UpsertInvocation implements Invocation {
   private final FunctionalMap.ReadWriteMap<Object, Object> rwMap;
   private final Object key;
   private final VersionedEntry versionedEntry;

   public UpsertInvocation(FunctionalMap.ReadWriteMap<Object, Object> rwMap, Object key, VersionedEntry versionedEntry) {
      this.rwMap = rwMap;
      this.key = key;
      this.versionedEntry = versionedEntry;
   }

   @Override
   public CompletableFuture<Void> invoke(boolean success) {
      if (success) {
         return rwMap.eval(key, versionedEntry);
      } else {
         return null;
      }
   }
}
