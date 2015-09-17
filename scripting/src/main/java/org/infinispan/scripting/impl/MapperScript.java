package org.infinispan.scripting.impl;

import javax.script.Bindings;
import javax.script.SimpleBindings;

import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.distexec.mapreduce.Collector;
import org.infinispan.distexec.mapreduce.Mapper;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.scripting.ScriptingManager;

/**
 * MapperScript.
 *
 * @author Tristan Tarrant
 * @since 7.2
 * @deprecated Use the streaming API within a local script instead
 */
@Deprecated
public class MapperScript<KIn, VIn, KOut, VOut> implements Mapper<KIn, VIn, KOut, VOut>, EnvironmentAware {
   private final ScriptMetadata metadata;
   private transient ScriptingManagerImpl scriptManager;
   private transient Bindings bindings;

   public MapperScript(ScriptMetadata metadata) {
      this.metadata = metadata;
   }

   @Override
   public void map(KIn key, VIn value, Collector<KOut, VOut> collector) {
      bindings.put("key", key);
      bindings.put("value", value);
      bindings.put("collector", collector);
      scriptManager.execute(metadata, bindings);
   }

   @Override
   public void setEnvironment(EmbeddedCacheManager cacheManager, Marshaller marshaller) {
      scriptManager = (ScriptingManagerImpl) SecurityActions.getGlobalComponentRegistry(cacheManager).getComponent(ScriptingManager.class);
      bindings = new SimpleBindings();
      bindings.put(SystemBindings.CACHE_MANAGER.toString(), cacheManager);
      bindings.put(SystemBindings.MARSHALLER.toString(), marshaller);
   }

}
