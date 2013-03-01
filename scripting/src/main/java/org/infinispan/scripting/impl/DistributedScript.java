package org.infinispan.scripting.impl;

import java.io.Serializable;
import java.util.Set;

import javax.script.Bindings;
import javax.script.SimpleBindings;

import org.infinispan.Cache;
import org.infinispan.distexec.DistributedCallable;
import org.infinispan.scripting.ScriptingManager;

/**
 * DistributedScript.
 *
 * @author Tristan Tarrant
 * @since 7.2
 */
class DistributedScript<T> implements DistributedCallable<Object, Object, T>, Serializable {
   private final ScriptMetadata metadata;
   private transient ScriptingManagerImpl scriptManager;
   private transient Bindings bindings;

   DistributedScript(ScriptMetadata metadata) {
      this.metadata = metadata;
   }

   @Override
   public T call() throws Exception {
      return (T) (scriptManager.execute(metadata, bindings).get());
   }

   @Override
   public void setEnvironment(Cache<Object, Object> cache, Set<Object> inputKeys) {
      scriptManager = (ScriptingManagerImpl) SecurityActions.getGlobalComponentRegistry(cache.getCacheManager()).getComponent(ScriptingManager.class);
      bindings = new SimpleBindings();
      bindings.put("inputKeys", inputKeys);
      bindings.put("cache", cache);
   }
}