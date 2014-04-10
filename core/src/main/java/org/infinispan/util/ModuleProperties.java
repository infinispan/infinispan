package org.infinispan.util;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.module.ExtendedModuleCommandFactory;
import org.infinispan.commands.module.ModuleCommandExtensions;
import org.infinispan.commands.module.ModuleCommandFactory;
import org.infinispan.commands.module.ModuleCommandInitializer;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commons.util.InfinispanCollections;
import org.infinispan.commons.util.ServiceFinder;
import org.infinispan.factories.components.ModuleMetadataFileFinder;
import org.infinispan.lifecycle.ModuleLifecycle;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

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

   public static Collection<ModuleLifecycle> resolveModuleLifecycles(ClassLoader cl) {
      return ServiceFinder.load(ModuleLifecycle.class, cl);
   }

   /**
    * Retrieves an Iterable containing metadata file finders declared by each module.
    * @param cl class loader to use
    * @return an Iterable of ModuleMetadataFileFinders
    */
   public static Iterable<ModuleMetadataFileFinder> getModuleMetadataFiles(ClassLoader cl) {
      return ServiceFinder.load(ModuleMetadataFileFinder.class, cl);
   }

   @SuppressWarnings("unchecked")
   public void loadModuleCommandHandlers(ClassLoader cl) {
      Collection<ModuleCommandExtensions> moduleCmdExtLoader =
            ServiceFinder.load(ModuleCommandExtensions.class, cl);

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
         commandInitializers = InfinispanCollections.emptyMap();
         commandFactories = InfinispanCollections.emptyMap();
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
         return InfinispanCollections.emptySet();

      Collection<Class<? extends CacheRpcCommand>> cacheRpcCmds = new HashSet<Class<? extends CacheRpcCommand>>(2);
      for (Class<? extends ReplicableCommand> moduleCmdClass : cmds) {
         if (CacheRpcCommand.class.isAssignableFrom(moduleCmdClass))
            cacheRpcCmds.add((Class<? extends CacheRpcCommand>) moduleCmdClass);
      }

      return cacheRpcCmds;
   }

   public Collection<Class<? extends ReplicableCommand>> moduleOnlyReplicableCommands() {
      Collection<Class<? extends ReplicableCommand>> cmds = moduleCommands();
      if (cmds == null || cmds.isEmpty())
         return InfinispanCollections.emptySet();

      Collection<Class<? extends ReplicableCommand>> replicableOnlyCmds =
            new HashSet<Class<? extends ReplicableCommand>>(2);
      for (Class<? extends ReplicableCommand> moduleCmdClass : cmds) {
         if (!CacheRpcCommand.class.isAssignableFrom(moduleCmdClass)) {
            replicableOnlyCmds.add(moduleCmdClass);
         }
      }
      return replicableOnlyCmds;
   }

}
