package org.infinispan.lucene;

import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.lifecycle.AbstractModuleLifecycle;
import org.infinispan.commons.marshall.AdvancedExternalizer;

import java.util.Map;

/**
 * Module lifecycle callbacks implementation that enables module specific
 * {@link org.infinispan.marshall.AdvancedExternalizer} implementations to be registered.
 *
 * @author Galder Zamarreño
 * @author Sanne Grinovero
 * @since 5.0
 */
public class LifecycleCallbacks extends AbstractModuleLifecycle {

   @Override
   public void cacheManagerStarting(GlobalComponentRegistry gcr, GlobalConfiguration globalCfg) {
      Map<Integer,AdvancedExternalizer<?>> externalizerMap = globalCfg.serialization().advancedExternalizers();
      externalizerMap.put(ExternalizerIds.CHUNK_CACHE_KEY, new ChunkCacheKey.Externalizer());
      externalizerMap.put(ExternalizerIds.FILE_CACHE_KEY, new FileCacheKey.Externalizer());
      externalizerMap.put(ExternalizerIds.FILE_LIST_CACHE_KEY, new FileListCacheKey.Externalizer());
      externalizerMap.put(ExternalizerIds.FILE_METADATA, new FileMetadata.Externalizer());
      externalizerMap.put(ExternalizerIds.FILE_READLOCK_KEY, new FileReadLockKey.Externalizer());
   }

}
