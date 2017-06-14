package org.infinispan.tree.impl;

import java.util.Map;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.lifecycle.ModuleLifecycle;
import org.infinispan.tree.Fqn;
import org.kohsuke.MetaInfServices;

/**
 * Module lifecycle callbacks implementation that enables module specific
 * {@link org.infinispan.marshall.AdvancedExternalizer} implementations to be registered.
 *
 * Information about the valid id range can be found <a href="http://community.jboss.org/docs/DOC-16198">here</a>
 *
 * @author Galder Zamarreño
 * @since 5.0
 */
@MetaInfServices(org.infinispan.lifecycle.ModuleLifecycle.class)
public class LifecycleCallbacks implements ModuleLifecycle {

   @Override
   public void cacheManagerStarting(GlobalComponentRegistry gcr, GlobalConfiguration globalCfg) {
      Map<Integer,AdvancedExternalizer<?>> externalizerMap = globalCfg.serialization().advancedExternalizers();
      externalizerMap.put(1000, new NodeKey.Externalizer());
      externalizerMap.put(1001, new Fqn.Externalizer());
   }

}
