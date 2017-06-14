package org.infinispan.multimap.impl;

import java.util.Map;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.lifecycle.ModuleLifecycle;
import org.infinispan.multimap.impl.function.ContainsFunction;
import org.infinispan.multimap.impl.function.GetFunction;
import org.infinispan.multimap.impl.function.PutFunction;
import org.infinispan.multimap.impl.function.RemoveFunction;
import org.kohsuke.MetaInfServices;

/**
 * MultimapModuleLifecycle is necessary for the Multimap Cache module.
 * Registers advanced externalizers.
 *
 * @author Katia Aresti - karesti@redhat.com
 * @since 9.2
 */
@MetaInfServices(value = ModuleLifecycle.class)
public class MultimapModuleLifecycle implements ModuleLifecycle {

   private static void addAdvancedExternalizer(Map<Integer, AdvancedExternalizer<?>> map, AdvancedExternalizer<?> ext) {
      map.put(ext.getId(), ext);
   }

   @Override
   public void cacheManagerStarting(GlobalComponentRegistry gcr, GlobalConfiguration globalConfiguration) {
      final Map<Integer, AdvancedExternalizer<?>> externalizerMap = globalConfiguration.serialization()
            .advancedExternalizers();

      addAdvancedExternalizer(externalizerMap, PutFunction.EXTERNALIZER);
      addAdvancedExternalizer(externalizerMap, RemoveFunction.EXTERNALIZER);
      addAdvancedExternalizer(externalizerMap, ContainsFunction.EXTERNALIZER);
      addAdvancedExternalizer(externalizerMap, GetFunction.EXTERNALIZER);
   }
}
