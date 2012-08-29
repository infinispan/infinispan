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
package org.infinispan.commands.module;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.factories.annotations.SurvivesRestarts;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;

/**
 * Modules which wish to implement their own commands and visitors must also provide an implementation of this interface
 * and declare it in their <tt>infinispan-module.properties</tt> file under key <tt>infinispan.module.command.initializer</tt>.
 * <p />
 * Implementations <b>must</b> be public classes with a public, no-arg constructor for instantiation.
 * <p />
 * @author Manik Surtani
 * @since 5.0
 */
@Scope(Scopes.NAMED_CACHE)
@SurvivesRestarts
public interface ModuleCommandInitializer {
   /**
    * Initializes a command constructed using {@link ModuleCommandFactory#fromStream(byte, Object[])} with
    * necessary named-cache-specific components.
    *
    * @param c command to initialize
    * @param isRemote true if the source is a remote node in the cluster, false otherwise.
    */
   void initializeReplicableCommand(ReplicableCommand c, boolean isRemote);
}
