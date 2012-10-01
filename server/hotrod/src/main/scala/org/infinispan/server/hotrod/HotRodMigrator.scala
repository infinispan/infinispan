/*
 * Copyright 2012 Red Hat, Inc. and/or its affiliates.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA
 */

/**
 * An implementation of Migrator, that understands the Hot Rod key and value formats.
 *
 * @author Manik Surtani
 * @since 5.2
 */

package org.infinispan.server.hotrod

import org.infinispan.factories.annotations.{Inject, SurvivesRestarts}
import org.infinispan.factories.scopes.{Scope, Scopes}
import org.infinispan.jmx.annotations.{ManagedOperation, MBean}
import org.infinispan.util.ByteArrayKey
import org.infinispan.server.core.{Operation, CacheValue}
import org.infinispan.Cache
import org.rhq.helpers.pluginAnnotations.agent.Operation
import org.infinispan.server.core.Operation
import org.infinispan.distexec.{DistributedCallable, DefaultExecutorService}
import java.nio.charset.Charset
import org.infinispan.marshall.jboss.JBossMarshaller
import org.infinispan.upgrade.Migrator

class HotRodMigrator(cache: Cache[ByteArrayKey, CacheValue]) extends Migrator {
   val KNOWN_KEY = "___MigrationManager_HotRod_KnownKeys___"
   val MARSHALLER = new JBossMarshaller // TODO: Hard coded!  Yuck!  Assumes the Synchronizer service will use the same marshaller.  Doesn't matter what actual clients use to store/retrieve data.

   @Override
   def getCacheName = cache.getName

   @Override
   def recordKnownGlobalKeyset() {
      // TODO: Maybe we should allow passing in of a well-known key prefix, and return the generated key to use?
      recordKnownGlobalKeyset(KNOWN_KEY)
   }

   def recordKnownGlobalKeyset(keyToRecordKnownKeySet: String) {

      // TODO: the bulk of ths code is reusable across different implementations of Migrator
      val bak = new ByteArrayKey(MARSHALLER.objectToByteBuffer(keyToRecordKnownKeySet))

      val cm = cache.getCacheConfiguration().clustering().cacheMode()
      var keys: java.util.HashSet[ByteArrayKey] = null
      if (cm.isReplicated() || !cm.isClustered()) {
         // If cache mode is LOCAL or REPL, dump local keyset.
         // Defensive copy to serialize and transmit across a network
         keys = new java.util.HashSet[ByteArrayKey](cache.keySet())
      } else {
         // If cache mode is DIST, use a map/reduce task
         val des = new DefaultExecutorService(cache)
         val keysets = des.submitEverywhere(new GlobalKeysetTask(cache))
         val combinedKeyset = new java.util.HashSet[ByteArrayKey]()

         for (future <- new IteratorWrapper(keysets.iterator())) {
            combinedKeyset addAll future.get()
         }
         keys = combinedKeyset
      }

      // Remove KNOWN_KEY from the key set - just in case it is there from a previous run.
      keys remove KNOWN_KEY

      // we cannot store the Set as it is; it will break if attempting to be read via Hot Rod.  This should be wrapped
      // in a CacheValue.
      cache.put(bak, new CacheValue(MARSHALLER.objectToByteBuffer(keys), 1))
   }
}

class IteratorWrapper[A](iter:java.util.Iterator[A]) {
   // TODO: should probably be in some generic scala helper module to allow scala-style iteration over java collections
   def foreach(f: A => Unit): Unit = {
      while(iter.hasNext){
         f(iter.next)
      }
   }
}

class GlobalKeysetTask(var cache: Cache[ByteArrayKey, CacheValue]) extends DistributedCallable[ByteArrayKey, CacheValue, java.util.Set[ByteArrayKey]] {
   // TODO this could possibly be moved elsewhere and reused
   @Override
   def call(): java.util.Set[ByteArrayKey] = {
      // Defensive copy to serialize and transmit across a network
      new java.util.HashSet(cache.keySet())
   }

   @Override
   def setEnvironment(cache: Cache[ByteArrayKey, CacheValue], inputKeys: java.util.Set[ByteArrayKey]) {
      this.cache = cache
   }
}
