package org.infinispan.jcache.embedded;

import java.util.Map;

import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.jcache.embedded.functions.GetAndPut;
import org.infinispan.jcache.embedded.functions.GetAndRemove;
import org.infinispan.jcache.embedded.functions.GetAndReplace;
import org.infinispan.jcache.embedded.functions.Invoke;
import org.infinispan.jcache.embedded.functions.MutableEntrySnapshot;
import org.infinispan.jcache.embedded.functions.Put;
import org.infinispan.jcache.embedded.functions.PutIfAbsent;
import org.infinispan.jcache.embedded.functions.ReadWithExpiry;
import org.infinispan.jcache.embedded.functions.Remove;
import org.infinispan.jcache.embedded.functions.RemoveConditionally;
import org.infinispan.jcache.embedded.functions.Replace;
import org.infinispan.jcache.embedded.functions.ReplaceConditionally;
import org.infinispan.commons.marshall.SingletonExternalizer;
import org.infinispan.commons.marshall.SuppliedExternalizer;
import org.infinispan.lifecycle.ModuleLifecycle;
import org.kohsuke.MetaInfServices;

@MetaInfServices(value = ModuleLifecycle.class)
public class LifecycleCallbacks implements ModuleLifecycle {
   private static Log log = LogFactory.getLog(LifecycleCallbacks.class);

   @Override
   public void cacheManagerStarting(GlobalComponentRegistry gcr, GlobalConfiguration globalConfiguration) {
      Map<Integer, AdvancedExternalizer<?>> map = globalConfiguration.serialization().advancedExternalizers();
      add(map, new SuppliedExternalizer<>(ExternalizerIds.READ_WITH_EXPIRY, ReadWithExpiry.class, ReadWithExpiry::new));
      add(map, new SuppliedExternalizer<>(ExternalizerIds.GET_AND_PUT, GetAndPut.class, GetAndPut::new));
      add(map, new SuppliedExternalizer<>(ExternalizerIds.GET_AND_REPLACE, GetAndReplace.class, GetAndReplace::new));
      add(map, new Invoke.Externalizer());
      add(map, new SuppliedExternalizer<>(ExternalizerIds.PUT, Put.class, Put::new));
      add(map, new SuppliedExternalizer<>(ExternalizerIds.PUT_IF_ABSENT, PutIfAbsent.class, PutIfAbsent::new));
      add(map, new SingletonExternalizer<>(ExternalizerIds.REMOVE, Remove.getInstance()));
      add(map, new SuppliedExternalizer<>(ExternalizerIds.REMOVE_CONDITIONALLY, RemoveConditionally.class, RemoveConditionally::new));
      add(map, new SuppliedExternalizer<>(ExternalizerIds.REPLACE, Replace.class, Replace::new));
      add(map, new SingletonExternalizer<>(ExternalizerIds.GET_AND_REMOVE, GetAndRemove.getInstance()));
      add(map, new ReplaceConditionally.Externalizer());
      // It is possible that the module is loaded (e.g. as a part of uberjar) but JCache is not on the classpath.
      // In that case we would experience problems as MutableEntrySnapshot extends JCache interface and without
      // that cannot be classloaded.
      if (canLoad("javax.cache.processor.MutableEntry", MutableEntrySnapshot.Externalizer.class.getClassLoader())) {
         add(map, new MutableEntrySnapshot.Externalizer());
      }
   }

   private boolean canLoad(String className, ClassLoader classLoader) {
      try {
         Util.loadClassStrict(className, classLoader);
         return true;
      } catch (ClassNotFoundException e) {
         log.tracef(e, "Cannot load " + className);
         return false;
      }
   }

   private static void add(Map<Integer, AdvancedExternalizer<?>> externalizerMap, AdvancedExternalizer<?> externalizer) {
      externalizerMap.put(externalizer.getId(), externalizer);
   }
}
