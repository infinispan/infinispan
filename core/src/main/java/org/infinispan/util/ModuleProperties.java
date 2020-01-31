package org.infinispan.util;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.module.ModuleCommandExtensions;
import org.infinispan.commands.module.ModuleCommandFactory;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commons.util.ServiceFinder;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * The <code>ModuleProperties</code> class represents Infinispan's module service extensions.
 *
 * @author Vladimir Blagojevic
 * @author Sanne Grinovero
 * @author Galder Zamarreño
 * @since 4.0
 * @deprecated Since 10.0, without replacement. To be removed very soon.
 */
@Deprecated
public final class ModuleProperties {

   private static final Log log = LogFactory.getLog(ModuleProperties.class);

   private Map<Byte, ModuleCommandFactory> commandFactories;
   private Collection<Class<? extends ReplicableCommand>> moduleCommands;

   public void loadModuleCommandHandlers(ClassLoader cl) {
      Collection<ModuleCommandExtensions> moduleCmdExtLoader = ServiceFinder.load(ModuleCommandExtensions.class, cl);

      if (moduleCmdExtLoader.iterator().hasNext()) {
         commandFactories = new HashMap<>(1);
         moduleCommands = new HashSet<>(1);
         for (ModuleCommandExtensions extension : moduleCmdExtLoader) {
            log.debugf("Loading module command extension SPI class: %s", extension);
            ModuleCommandFactory cmdFactory = extension.getModuleCommandFactory();
            Objects.requireNonNull(cmdFactory);
            for (Map.Entry<Byte, Class<? extends ReplicableCommand>> command : cmdFactory.getModuleCommands().entrySet()) {
               byte id = command.getKey();
               if (commandFactories.containsKey(id))
                  throw new IllegalArgumentException(String.format(
                        "Cannot use id %d for commands, as it is already in use by %s",
                        id, commandFactories.get(id).getClass().getName()));

               commandFactories.put(id, cmdFactory);
               moduleCommands.add(command.getValue());
            }
         }
      } else {
         log.debug("No module command extensions to load");
         commandFactories = Collections.emptyMap();
      }
   }

   public Collection<Class<? extends ReplicableCommand>> moduleCommands() {
      return moduleCommands;
   }

   public Map<Byte, ModuleCommandFactory> moduleCommandFactories() {
      return commandFactories;
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
