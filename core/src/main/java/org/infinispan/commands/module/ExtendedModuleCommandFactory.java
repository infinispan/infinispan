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

package org.infinispan.commands.module;

import org.infinispan.commands.remote.CacheRpcCommand;

/**
 * Temporary workaround to avoid modifying {@link ModuleCommandFactory}
 * interface. This interface should be merged with {@link ModuleCommandFactory}
 * in 6.0.
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
public interface ExtendedModuleCommandFactory extends ModuleCommandFactory {

   /**
    * Construct and initialize a {@link CacheRpcCommand} based on the command
    * id and argument array passed in.
    *
    * @param commandId  command id to construct
    * @param args       array of arguments with which to initialize the {@link CacheRpcCommand}
    * @param cacheName  cache name at which command to be created is directed
    * @return           a {@link CacheRpcCommand}
    */
   CacheRpcCommand fromStream(byte commandId, Object[] args, String cacheName);

}
