package org.infinispan.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.module.ModuleCommandExtensions;
import org.infinispan.commands.module.ModuleCommandFactory;
import org.infinispan.commands.module.ModuleCommandInitializer;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commons.util.ServiceFinder;
import org.infinispan.factories.components.ModuleMetadataFileFinder;
import org.infinispan.lifecycle.ModuleLifecycle;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * The <code>ModuleProperties</code> class represents Infinispan's module service extensions.
 *
 * @author Vladimir Blagojevic
 * @author Sanne Grinovero
 * @author Galder Zamarreño
 * @since 4.0
 */
public final class ModuleProperties {

   private static final Log log = LogFactory.getLog(ModuleProperties.class);

   private Map<Byte, ModuleCommandFactory> commandFactories;
   private Map<Byte, ModuleCommandInitializer> commandInitializers;
   private Collection<Class<? extends ReplicableCommand>> moduleCommands;

   private static final String QUERY_LIFECYCLE = "org.infinispan.query.impl.LifecycleManager";
   private static final String REMOTE_QUERY_LIFECYCLE = "org.infinispan.query.remote.impl.LifecycleManager";

   public static Collection<ModuleLifecycle> resolveModuleLifecycles(ClassLoader cl) {
      Collection<ModuleLifecycle> moduleLifecycles = ServiceFinder.load(ModuleLifecycle.class, cl);
      return addModuleDependency(moduleLifecycles, REMOTE_QUERY_LIFECYCLE, QUERY_LIFECYCLE);
   }

   /**
    * Change the order of the discovered modules to make sure dependencies are processed first.
    *
    * @param modules The resolved modules
    * @param moduleName The module name to add a dependency
    * @param dependencyName The dependency of the module, that must be processed first
    */
   private static Collection<ModuleLifecycle> addModuleDependency(Collection<ModuleLifecycle> modules,
                                                                  String moduleName, String dependencyName) {
      List<ModuleLifecycle> sorted = new ArrayList<>(modules);
      int mainIndex = -1, depIndex = -1;
      for (int i = 0; i < sorted.size(); i++) {
         String name = sorted.get(i).getClass().getName();
         if (moduleName.equals(name)) mainIndex = i;
         if (dependencyName.equals(name)) depIndex = i;
      }
      if (depIndex < mainIndex || mainIndex == -1) return modules;
      Collections.swap(sorted, mainIndex, depIndex);
      return sorted;
   }

   /**
    * Retrieves an Iterable containing metadata file finders declared by each module.
    * @param cl class loader to use
    * @return an Iterable of ModuleMetadataFileFinders
    */
   public static Iterable<ModuleMetadataFileFinder> getModuleMetadataFiles(ClassLoader cl) {
      return ServiceFinder.load(ModuleMetadataFileFinder.class, cl);
   }

   public void loadModuleCommandHandlers(ClassLoader cl) {
      Collection<ModuleCommandExtensions> moduleCmdExtLoader = ServiceFinder.load(ModuleCommandExtensions.class, cl);

      if (moduleCmdExtLoader.iterator().hasNext()) {
         commandFactories = new HashMap<>(1);
         commandInitializers = new HashMap<>(1);
         moduleCommands = new HashSet<>(1);
         for (ModuleCommandExtensions extension : moduleCmdExtLoader) {
            log.debugf("Loading module command extension SPI class: %s", extension);
            ModuleCommandFactory cmdFactory = extension.getModuleCommandFactory();
            Objects.requireNonNull(cmdFactory);
            ModuleCommandInitializer cmdInitializer = extension.getModuleCommandInitializer();
            Objects.requireNonNull(cmdInitializer);
            for (Map.Entry<Byte, Class<? extends ReplicableCommand>> command : cmdFactory.getModuleCommands().entrySet()) {
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
         log.debug("No module command extensions to load");
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

      Collection<Class<? extends CacheRpcCommand>> cacheRpcCmds = new HashSet<>(2);
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

      Collection<Class<? extends ReplicableCommand>> replicableOnlyCmds = new HashSet<>(2);
      for (Class<? extends ReplicableCommand> moduleCmdClass : cmds) {
         if (!CacheRpcCommand.class.isAssignableFrom(moduleCmdClass)) {
            replicableOnlyCmds.add(moduleCmdClass);
         }
      }
      return replicableOnlyCmds;
   }
}
