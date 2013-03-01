package org.infinispan.scripting.impl;

import java.util.Map;

import javax.script.SimpleBindings;

import org.infinispan.commons.CacheException;
import org.infinispan.distexec.mapreduce.Collator;

/**
 * CollatorScript.
 *
 * @author Tristan Tarrant
 * @since 7.2
 */
public class CollatorScript<KOut, VOut, R> implements Collator<KOut, VOut, R> {
   private final ScriptMetadata metadata;
   private final ScriptingManagerImpl scriptManager;

   public CollatorScript(ScriptMetadata metadata, ScriptingManagerImpl scriptManager) {
      this.metadata = metadata;
      this.scriptManager = scriptManager;
   }

   @Override
   public R collate(Map<KOut, VOut> reducedResults) {
      SimpleBindings bindings = new SimpleBindings();
      bindings.put(SystemBindings.CACHE_MANAGER.toString(), scriptManager.cacheManager);
      bindings.put("reducedResults", reducedResults);
      try {
         return (R) scriptManager.execute(metadata, bindings).get();
      } catch (Exception e) {
         throw new CacheException("Error during collator script", e);
      }
   }
}
