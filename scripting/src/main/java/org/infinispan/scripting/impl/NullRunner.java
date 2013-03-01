package org.infinispan.scripting.impl;

import java.lang.invoke.MethodHandles;

import org.infinispan.commons.util.concurrent.NotifyingFuture;
import org.infinispan.scripting.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * NullRunner.
 *
 * @author Tristan Tarrant
 * @since 7.2
 */
public class NullRunner implements ScriptRunner {
   final static Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass(), Log.class);
   public static final NullRunner INSTANCE = new NullRunner();

   private NullRunner() {
   }

   @Override
   public <T> NotifyingFuture<T> runScript(ScriptingManagerImpl scriptManager, ScriptMetadata metadata, CacheScriptBindings binding) {
      throw log.cannotInvokeScriptDirectly(metadata.name(), metadata.mode().toString());
   }

}
