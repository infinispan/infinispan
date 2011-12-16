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
package org.infinispan.util;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.module.ExtendedModuleCommandFactory;
import org.infinispan.commands.module.ModuleCommandExtensions;
import org.infinispan.commands.module.ModuleCommandFactory;
import org.infinispan.commands.module.ModuleCommandInitializer;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.factories.components.ModuleMetadataFileFinder;
import org.infinispan.lifecycle.ModuleLifecycle;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.ServiceLoader;

/**
 * The <code>ModuleProperties</code> class represents Infinispan's module service extensions
 *
 * @author Vladimir Blagojevic
 * @author Sanne Grinovero
 * @author Galder Zamarreño
 * @since 4.0
 */
public class ModuleProperties extends Properties {

   private static final long serialVersionUID = 2558131508076199744L;

   private static final Log log = LogFactory.getLog(ModuleProperties.class);

   private Map<Byte, ModuleCommandFactory> commandFactories;
   private Map<Byte, ModuleCommandInitializer> commandInitializers;
   private Collection<Class<? extends ReplicableCommand>> moduleCommands;

   public List<ModuleLifecycle> resolveModuleLifecycles(ClassLoader cl) {
      ServiceLoader<ModuleLifecycle> moduleLifecycleLoader =
            ServiceLoader.load(ModuleLifecycle.class, cl);

      if (moduleLifecycleLoader.iterator().hasNext()) {
         List<ModuleLifecycle> lifecycles = new LinkedList<ModuleLifecycle>();
         for (ModuleLifecycle lifecycle : moduleLifecycleLoader) {
            log.debugf("Loading lifecycle SPI class: %s", lifecycle);
            lifecycles.add(lifecycle);
         }
         return lifecycles;
      } else {
         log.debugf("No module lifecycle SPI classes available");
         return Collections.emptyList();
      }
   }

   /**
    * Retrieves an Iterable containing metadata file finders declared by each module.
    * @param cl class loader to use
    * @return an Iterable of ModuleMetadataFileFinders
    */
   public Iterable<ModuleMetadataFileFinder> getModuleMetadataFiles(ClassLoader cl) {
      return ServiceLoader.load(ModuleMetadataFileFinder.class, cl);
   }

   @SuppressWarnings("unchecked")
   public void loadModuleCommandHandlers(ClassLoader cl) {
      ServiceLoader<ModuleCommandExtensions> moduleCmdExtLoader =
            ServiceLoader.load(ModuleCommandExtensions.class, cl);

      if (moduleCmdExtLoader.iterator().hasNext()) {
         commandFactories = new HashMap<Byte, ModuleCommandFactory>(1);
         commandInitializers = new HashMap<Byte, ModuleCommandInitializer>(1);
         moduleCommands = new HashSet<Class<? extends ReplicableCommand>>(1);
         for (ModuleCommandExtensions extension : moduleCmdExtLoader) {
            log.debugf("Loading module command extension SPI class: %s", extension);
            ExtendedModuleCommandFactory cmdFactory = extension.getModuleCommandFactory();
            ModuleCommandInitializer cmdInitializer = extension.getModuleCommandInitializer();
            for (Map.Entry<Byte, Class<? extends ReplicableCommand>> command :
                  cmdFactory.getModuleCommands().entrySet()) {
               byte id = command.getKey();
               if (commandFactories.containsKey(id))
                  throw new IllegalArgumentException(String.format(
                        "Cannot use id %d for commands, as it is already in use by %s",
                        id, commandFactories.get(id).getClass().getName()));

               commandFactories.put(id, cmdFactory);
               moduleCommands.add(command.getValue());
               commandInitializers.put(id, cmdInitializer);
            }
         }
      } else {
         log.debugf("No module command extensions to load");
         commandInitializers = Collections.emptyMap();
         commandFactories = Collections.emptyMap();
      }
   }

   public Collection<Class<? extends ReplicableCommand>> moduleCommands() {
      return moduleCommands;
   }

   public Map<Byte, ModuleCommandFactory> moduleCommandFactories() {
      return commandFactories;
   }

   public Map<Byte, ModuleCommandInitializer> moduleCommandInitializers() {
      return commandInitializers;
   }

   @SuppressWarnings("unchecked")
   public Collection<Class<? extends CacheRpcCommand>> moduleCacheRpcCommands() {
      Collection<Class<? extends ReplicableCommand>> cmds = moduleCommands();
      if (cmds == null || cmds.isEmpty())
         return Collections.emptySet();

      Collection<Class<? extends CacheRpcCommand>> cacheRpcCmds = new HashSet<Class<? extends CacheRpcCommand>>();
      for (Class<? extends ReplicableCommand> moduleCmdClass : cmds) {
         if (CacheRpcCommand.class.isAssignableFrom(moduleCmdClass))
            cacheRpcCmds.add((Class<? extends CacheRpcCommand>) moduleCmdClass);
      }

      return cacheRpcCmds;
   }

   public Collection<Class<? extends ReplicableCommand>> moduleOnlyReplicableCommands() {
      Collection<Class<? extends ReplicableCommand>> cmds = moduleCommands();
      if (cmds == null || cmds.isEmpty())
         return Collections.emptySet();

      Collection<Class<? extends ReplicableCommand>> replicableOnlyCmds =
            new HashSet<Class<? extends ReplicableCommand>>();
      for (Class<? extends ReplicableCommand> moduleCmdClass : cmds) {
         if (!CacheRpcCommand.class.isAssignableFrom(moduleCmdClass)) {
            replicableOnlyCmds.add(moduleCmdClass);
         }
      }
      return replicableOnlyCmds;
   }

}
