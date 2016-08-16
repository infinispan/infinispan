package org.infinispan.functional.impl;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.infinispan.commons.api.functional.MetaParam;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.util.Experimental;
import org.infinispan.commons.util.Util;
import org.infinispan.marshall.core.Ids;

import net.jcip.annotations.NotThreadSafe;

/**
 * Represents a {@link MetaParam} collection.
 *
 * <p>In {@link Params}, the internal array where each parameter was
 * stored is indexed by an integer. This worked fine because the available
 * parameters are exclusively controlled by the Infinispan. This is not the
 * case with {@link MetaParam} instances where users are expected to add their
 * own types. So, for {@link MetaParams}, an array is still used but the lookup
 * is done sequentially comparing the type of the {@link MetaParam} looked for
 * against the each individual {@link MetaParam} instance stored in
 * {@link MetaParams}.
 *
 * <p>Having sequential {@link MetaParam} lookups over an array is O(n),
 * but this is not problematic since the number of {@link MetaParam} to be
 * stored with each cached entry is expected to be small, less than 10 per
 * {@link MetaParams} collection. So, the performance impact is quite small.
 *
 * <p>Storing {@link MetaParam} instances in an array adds the least
 * amount of overhead to keeping a collection of {@link MetaParam} in memory
 * along with each cached entry, while retaining flexibility to add or remove
 * {@link MetaParam} instances.
 *
 * <p>This {@link MetaParams} collection is not thread safe because
 * it is expected that any updates will be done having acquired write locks
 * on the entire {@link org.infinispan.container.entries.CacheEntry} which
 * references the {@link MetaParams} collection. Hence, any updates could be
 * done without the need to keep {@link MetaParams} concurrently safe.
 * Also, although users can retrieve or update individual {@link MetaParam}
 * instances, they cannot act on the globally at the {@link MetaParams} level,
 * and hence there is no risk of users misusing {@link MetaParams}.
 *
 * @since 8.0
 */
@NotThreadSafe
@Experimental
public final class MetaParams {

   private MetaParam<?>[] metas;

   private MetaParams(MetaParam<?>[] metas) {
      this.metas = metas;
   }

   public boolean isEmpty() {
      return metas.length == 0;
   }

   public int size() {
      return metas.length;
   }

   public <T> Optional<T> find(Class<T> type) {
      return Optional.ofNullable(findNullable(type));
   }

   @SuppressWarnings("unchecked")
   private <T> T findNullable(Class<T> type) {
      for (MetaParam<?> meta : metas) {
         if (meta.getClass().isAssignableFrom(type))
            return (T) meta;
      }

      return null;
   }

   public void add(MetaParam.Writable meta) {
      if (metas.length == 0)
         metas = new MetaParam[]{meta};
      else {
         boolean found = false;
         for (int i = 0; i < metas.length; i++) {
            if (metas[i].getClass().isAssignableFrom(meta.getClass())) {
               metas[i] = meta;
               found = true;
            }
         }

         if (!found) {
            MetaParam<?>[] newMetas = Arrays.copyOf(metas, metas.length + 1);
            newMetas[newMetas.length - 1] = meta;
            metas = newMetas;
         }
      }
   }

   public void addMany(MetaParam.Writable... metaParams) {
      if (metas.length == 0) metas = metaParams;
      else {
         List<MetaParam<?>> notFound = new ArrayList<>(metaParams.length);
         for (MetaParam.Writable newMeta : metaParams) {
            boolean found = false;
            for (int i = 0; i < metas.length; i++) {
               if (metas[i].getClass().isAssignableFrom(newMeta.getClass())) {
                  metas[i] = newMeta;
                  found = true;
               }
            }
            if (!found)
               notFound.add(newMeta);
         }

         if (!notFound.isEmpty()) {
            List<MetaParam<?>> allMetasList = new ArrayList<>(Arrays.asList(metas));
            allMetasList.addAll(notFound);
            metas = allMetasList.toArray(new MetaParam[metas.length + notFound.size()]);
         }
      }
   }

   @Override
   public String toString() {
      return "MetaParams{" +
         "metas=" + Arrays.toString(metas) +
         '}';
   }

   /**
    * Construct a collection of {@link MetaParam} instances. If multiple
    * instances of the same {@link MetaParam} are present, the last value is
    * only considered since there can only be one instance per type in the
    * {@link MetaParams} collection.
    *
    * @param metas Meta parameters to create the collection with
    * @return a collection of meta parameters without type duplicates
    */
   static MetaParams of(MetaParam... metas) {
      return new MetaParams(filterDuplicates(metas));
   }

   static MetaParams of(MetaParam meta) {
      return new MetaParams(new MetaParam[]{meta});
   }

   static MetaParams empty() {
      return new MetaParams(new MetaParam[]{});
   }

   private static MetaParam[] filterDuplicates(MetaParam... metas) {
      Map<Class<?>, MetaParam<?>> all = new HashMap<>();
      for (MetaParam meta : metas)
         all.put(meta.getClass(), meta);

      return all.values().toArray(new MetaParam[all.size()]);
   }

   public static final class Externalizer extends AbstractExternalizer<MetaParams> {
      @Override
      public void writeObject(ObjectOutput oo, MetaParams o) throws IOException {
         oo.writeInt(o.metas.length);
         for (Object meta : o.metas) oo.writeObject(meta);
      }

      @Override
      public MetaParams readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         int length = input.readInt();
         MetaParam[] metas = new MetaParam[length];
         for (int i = 0; i < length; i++)
            metas[i] = (MetaParam) input.readObject();

         return MetaParams.of(metas);
      }

      @Override
      public Set<Class<? extends MetaParams>> getTypeClasses() {
         return Util.<Class<? extends MetaParams>>asSet(MetaParams.class);
      }

      @Override
      public Integer getId() {
         return Ids.META_PARAMS;
      }
   }
}
