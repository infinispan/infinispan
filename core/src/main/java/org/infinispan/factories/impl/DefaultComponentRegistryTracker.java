package org.infinispan.factories.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.infinispan.commons.time.TimeService;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.util.logging.events.Messages;
import org.jboss.logging.Logger;

final class DefaultComponentRegistryTracker implements ComponentRegistryTracker {

   private static final Log LOG = LogFactory.getLog(DefaultComponentRegistryTracker.class);

   // TODO: Update to a flag checking whether safe-mode is enabled.
   private static final boolean LOG_ENABLED = Boolean.getBoolean("org.infinispan.component.tracker");

   private final Map<String, ComponentEntry> components = new ConcurrentHashMap<>();
   private final BasicComponentRegistry registry;
   private final String scope;
   private TimeService timeService;

   DefaultComponentRegistryTracker(BasicComponentRegistry registry, boolean isGlobal) {
      this.registry = registry;
      this.scope = isGlobal ? "GLOBAL" : null;
      this.timeService = isGlobal ? null : registry.getComponent(TimeService.class).wired();
   }

   @Override
   public void stateChanged(String componentName, BasicComponentRegistryImpl.WrapperState state, BasicComponentRegistryImpl.ComponentPath path) {
      if (timeService == null && state == BasicComponentRegistryImpl.WrapperState.WIRED && componentName.equals(GlobalConfiguration.class.getName())) {
         timeService = registry.getComponent(TimeService.class).wired();
      }

      ComponentEntry entry = components.computeIfAbsent(componentName,
            name -> new ComponentEntry(name, state, accessors(path)));

      // Transition to the most up-to-date state and records the time.
      entry.state(state, timeService);
      logTransition(entry);
   }

   @Override
   public Collection<ComponentEntry> entries() {
      return Collections.unmodifiableCollection(components.values());
   }

   @Override
   public void clear() {
      components.clear();
   }

   @Override
   public void removeComponent(String componentName) {
      components.remove(componentName);
   }

   private void logTransition(ComponentEntry entry) {
      String scope = this.scope != null ? this.scope : resolveCacheName();
      long phaseMs = entry.duration(TimeUnit.MILLISECONDS);
      BasicComponentRegistryImpl.WrapperState state = entry.state();

      if (state == BasicComponentRegistryImpl.WrapperState.STARTED && phaseMs > 0) {
         Logger.Level level = LOG_ENABLED ? Logger.Level.INFO : Logger.Level.TRACE;
         String message = String.format("%s %s", Messages.MESSAGES.eventLogContext(scope), Messages.MESSAGES.cacheComponentPhaseCompleted(entry.name(), state.name(), phaseMs));
         LOG.log(level, message);
      }
   }

   private String resolveCacheName() {
      try {
         ComponentRef<String> ref = registry.getComponent(KnownComponentNames.CACHE_NAME, String.class);
         if (ref != null) {
            String name = ref.wired();
            if (name != null)
               return name;
         }
      } catch (Exception ignored) {
         // Cache name not available yet.
      }
      return "-";
   }

   private static List<String> accessors(BasicComponentRegistryImpl.ComponentPath path) {
      if (path == null) return Collections.emptyList();

      // The first component is the component we are currently initializing.
      BasicComponentRegistryImpl.ComponentPath p = path.next;
      if (p == null) return Collections.emptyList();

      List<String> results = new ArrayList<>();
      while (p != null) {
         String label = p.className != null ? p.className : p.name;
         results.add(label);
         p = p.next;
      }

      return results;
   }
}
