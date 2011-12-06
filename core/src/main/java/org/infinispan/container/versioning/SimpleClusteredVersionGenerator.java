/*
 * Copyright 2011 Red Hat, Inc. and/or its affiliates.
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

package org.infinispan.container.versioning;

import org.infinispan.Cache;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachemanagerlistener.annotation.ViewChanged;
import org.infinispan.notifications.cachemanagerlistener.event.ViewChangedEvent;
import org.infinispan.remoting.transport.Transport;

/**
 * A version generator implementation for SimpleClusteredVersions
 *
 * @author Manik Surtani
 * @since 5.1
 */
public class SimpleClusteredVersionGenerator implements VersionGenerator {
   // The current cluster view ID is recorded and used as a part of the version generated, and as such used as the
   // most significant part of a version comparison.  If a version is generated based on an old view and another is
   // generated based on a newer view, the one based on the newer view wins regardless of the version's counter.
   // See SimpleClusteredVersion for more details.
   private volatile int viewId;
   private Cache cache;
   private Transport transport;

   @Inject
   public void init(Cache cache, Transport transport) {
      this.cache = cache;
      this.transport = transport;
   }

   @Start(priority = 11) // needs to happen *after* the transport starts.
   public void start() {
      if (transport != null) {
         viewId = transport.getViewId();
         cache.getCacheManager().addListener(new ViewIdUpdater());
      }
   }

   @Override
   public IncrementableEntryVersion generateNew() {
      return new SimpleClusteredVersion(viewId, 1);
   }

   @Override
   public IncrementableEntryVersion increment(IncrementableEntryVersion initialVersion) {
      if (initialVersion instanceof SimpleClusteredVersion) {
         SimpleClusteredVersion old = (SimpleClusteredVersion) initialVersion;
         return new SimpleClusteredVersion(viewId, old.version + 1);
      } else {
         throw new IllegalArgumentException("I only know how to deal with SimpleClusteredVersions, not " + initialVersion.getClass().getName());
      }
   }

   @Listener
   public class ViewIdUpdater {
      @ViewChanged
      public void handleViewChange(ViewChangedEvent e) {
         viewId = e.getViewId();
      }
   }
}
