package org.infinispan.cli.interpreter;

import org.infinispan.cli.interpreter.logging.Log;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.annotations.InfinispanModule;
import org.infinispan.factories.impl.BasicComponentRegistry;
import org.infinispan.jmx.CacheManagerJmxRegistration;
import org.infinispan.lifecycle.ModuleLifecycle;
import org.infinispan.util.logging.LogFactory;

@InfinispanModule(name = "cli-interpreter", requiredModules = "core")
public class LifecycleCallbacks implements ModuleLifecycle {

   private static final Log log = LogFactory.getLog(LifecycleCallbacks.class, Log.class);

   @Override
   public void cacheManagerStarting(GlobalComponentRegistry gcr, GlobalConfiguration globalConfiguration) {
      if (globalConfiguration.statistics()) {
         BasicComponentRegistry bcr = gcr.getComponent(BasicComponentRegistry.class);

         Interpreter interpreter = new Interpreter();
         bcr.registerComponent(Interpreter.class, interpreter, true);

         try {
            CacheManagerJmxRegistration jmxRegistration = bcr.getComponent(CacheManagerJmxRegistration.class).running();
            jmxRegistration.registerMBean(interpreter);
         } catch (Exception e) {
            throw log.jmxRegistrationFailed(e);
         }
      }
   }
}
