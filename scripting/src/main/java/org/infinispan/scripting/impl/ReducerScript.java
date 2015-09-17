package org.infinispan.scripting.impl;

import java.util.Iterator;

import javax.script.SimpleBindings;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.distexec.mapreduce.Reducer;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.scripting.ScriptingManager;

/**
 * ReducerScript.
 *
 * @author Tristan Tarrant
 * @since 7.2
 * @deprecated Use the streaming API within a local script instead
 */
@Deprecated
public class ReducerScript<KOut, VOut> implements Reducer<KOut, VOut>, EnvironmentAware {
   private final ScriptMetadata metadata;
   private transient ScriptingManagerImpl scriptManager;
   private transient SimpleBindings bindings;

   public ReducerScript(ScriptMetadata metadata) {
      this.metadata  = metadata;
   }

   @Override
   public VOut reduce(KOut reducedKey, Iterator<VOut> iter) {
      bindings.put("reducedKey", reducedKey);
      bindings.put("iter", iter);
      try {
         return (VOut) scriptManager.execute(metadata, bindings).get();
      } catch (Exception e) {
         throw new CacheException("Error during reducer script", e);
      }
   }

   @Override
   public void setEnvironment(EmbeddedCacheManager cacheManager, Marshaller marshaller) {
      scriptManager = (ScriptingManagerImpl) SecurityActions.getGlobalComponentRegistry(cacheManager).getComponent(ScriptingManager.class);
      bindings = new SimpleBindings();
      bindings.put(SystemBindings.CACHE_MANAGER.toString(), cacheManager);
      bindings.put(SystemBindings.MARSHALLER.toString(), marshaller);
   }

}
