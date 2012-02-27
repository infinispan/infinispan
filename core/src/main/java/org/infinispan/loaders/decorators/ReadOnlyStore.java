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
package org.infinispan.loaders.decorators;

import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.loaders.CacheStore;
import org.infinispan.loaders.modifications.Modification;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.ObjectInput;
import java.util.List;

/**
 * A decorator that makes the underlying store a {@link org.infinispan.loaders.CacheLoader}, i.e., suppressing all write
 * methods.
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class ReadOnlyStore extends AbstractDelegatingStore {
   private static final Log log = LogFactory.getLog(ReadOnlyStore.class);

   public ReadOnlyStore(CacheStore delegate) {
      super(delegate);
   }

   @Override
   public void store(InternalCacheEntry ed) {
      log.trace("Ignoring store invocation"); 
   }

   @Override
   public void fromStream(ObjectInput inputStream) {
      log.trace("Ignoring writing contents of stream to store");
   }

   @Override
   public void clear() {
      log.trace("Ignoring clear invocation");
   }

   @Override
   public boolean remove(Object key) {
      log.trace("Ignoring removal of key");
      return false;  // no-op
   }

   @Override
   public void purgeExpired() {
      log.trace("Ignoring purge expired invocation");
   }

   @Override
   public void commit(GlobalTransaction tx) {
      log.trace("Ignoring transactional commit call");
   }

   @Override
   public void rollback(GlobalTransaction tx) {
      log.trace("Ignoring transactional rollback call");
   }

   @Override
   public void prepare(List<? extends Modification> list, GlobalTransaction tx, boolean isOnePhase) {
      log.trace("Ignoring transactional prepare call");
   }
}
