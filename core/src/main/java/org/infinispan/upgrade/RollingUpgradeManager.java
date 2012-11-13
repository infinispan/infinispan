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

package org.infinispan.upgrade;

import org.infinispan.factories.annotations.SurvivesRestarts;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.rhq.helpers.pluginAnnotations.agent.Operation;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * This component handles the control hooks to handle migrating from one version of Infinispan to another.
 *
 * @author Manik Surtani
 * @since 5.2
 */
@MBean(objectName = "RollingUpgradeManager", description = "This component handles the control hooks to handle migrating data from one version of Infinispan to another")
@Scope(value = Scopes.NAMED_CACHE)
@SurvivesRestarts
public class RollingUpgradeManager {
   private final ConcurrentMap<String, Migrator> migrators = new ConcurrentHashMap<String, Migrator>(2);
   @ManagedOperation(description = "Dumps the global known keyset to a well-known key for retrieval by the upgrade process")
   @Operation(displayName = "Dumps the global known keyset")
   public void recordKnownGlobalKeyset() {
      for (Migrator m: migrators.values()) m.recordKnownGlobalKeyset();
   }

   /**
    * Registers a migrator for a specific data format or endpoint.  In the Infinispan ecosystem, we'd typically have one
    * Migrator implementation for Hot Rod, one for memcached, one for REST and one for embedded/in-VM mode, and these
    * would typically be added to the upgrade manager on first access via any of these protocols.
    * @param migrator
    */
   public void addMigrator(Migrator migrator) {
      migrators.putIfAbsent(migrator.getCacheName(), migrator);
   }
}
