package org.infinispan.query.impl.massindex;

import org.infinispan.Cache;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.util.Util;
import org.infinispan.distexec.DistributedCallable;
import org.infinispan.query.backend.QueryInterceptor;
import org.infinispan.query.impl.ComponentRegistryUtils;
import org.infinispan.query.impl.externalizers.ExternalizerIds;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

/**
 * Used to clean indexes across the cluster.
 *
 * @author gustavonalle
 * @since 7.0
 */
public class IndexCleanCallable implements DistributedCallable<Void, Void, Void> {
   private QueryInterceptor queryInterceptor;

   @Override
   public Void call() throws Exception {
      queryInterceptor.purgeAllIndexes();
      return null;
   }

   @Override
   public void setEnvironment(Cache cache, Set inputKeys) {
      this.queryInterceptor = ComponentRegistryUtils.getQueryInterceptor(cache);
   }

   public static class Externalizer extends AbstractExternalizer<IndexCleanCallable> {

      @Override
      @SuppressWarnings("ALL")
      public Set<Class<? extends IndexCleanCallable>> getTypeClasses() {
         return Util.<Class<? extends IndexCleanCallable>>asSet(IndexCleanCallable.class);
      }

      @Override
      public void writeObject(ObjectOutput output, IndexCleanCallable object) throws IOException {
      }

      @Override
      public IndexCleanCallable readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new IndexCleanCallable();
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.INDEX_CLEAN_CALLABLE;
      }
   }

}
