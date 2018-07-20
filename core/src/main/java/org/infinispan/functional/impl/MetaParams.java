package org.infinispan.functional.impl;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Function;
import java.util.stream.Stream;

import org.infinispan.commons.util.Experimental;
import org.infinispan.functional.MetaParam;

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
 * This class should not be accessible from user code, therefore it is package-protected.
 *
 * @since 8.0
 */
@NotThreadSafe
@Experimental
public final class MetaParams implements Iterable<MetaParam<?>> {

   private static final MetaParam<?>[] EMPTY_ARRAY = {};
   private MetaParam<?>[] metas;
   private int length;

   public MetaParams() {
      this.metas = EMPTY_ARRAY;
      this.length = 0;
   }

   private MetaParams(MetaParam<?>[] metas, int length) {
      this.metas = metas;
      this.length = length;
      assert checkLength();
   }

   public boolean isEmpty() {
      return length == 0;
   }

   public int size() {
      return length;
   }

   public MetaParams copy() {
      if (length == 0) {
         return empty();
      } else {
         return new MetaParams(Arrays.copyOf(metas, metas.length), length);
      }
   }

   public <T extends MetaParam> Optional<T> find(Class<T> type) {
      return Optional.ofNullable(findNullable(type));
   }

   @SuppressWarnings("unchecked")
   private <T extends MetaParam> T findNullable(Class<T> type) {
      for (MetaParam<?> meta : metas) {
         if (meta != null && meta.getClass().isAssignableFrom(type))
            return (T) meta;
      }

      return null;
   }

   public void add(MetaParam meta) {
      assert meta != null;
      if (metas.length == 0) {
         metas = new MetaParam[]{meta};
         length = 1;
      } else {
         int hole = -1;
         for (int i = 0; i < metas.length; i++) {
            MetaParam<?> m = metas[i];
            if (m == null) {
               hole = i;
            } else if (m.getClass().isAssignableFrom(meta.getClass())) {
               metas[i] = meta;
               assert checkLength();
               return;
            }
         }

         ++length;
         if (hole < 0) {
            MetaParam<?>[] newMetas = Arrays.copyOf(metas, metas.length + 1);
            newMetas[newMetas.length - 1] = meta;
            metas = newMetas;
         } else {
            metas[hole] = meta;
         }
         assert checkLength();
      }
   }

   private boolean checkLength() {
      int l = 0;
      for (MetaParam meta : metas) {
         if (meta != null) ++l;
      }
      return l == length;
   }

   public void addMany(MetaParam... metaParams) {
      if (metas.length == 0) {
         // Arrays in Java are covariant, therefore someone could pass MetaParam.Writable[] and
         // we could try to store non-writable meta param in that array (or its copy) later.
         if (metaParams.getClass().getComponentType() == MetaParam.class) {
            metas = metaParams;
         } else {
            metas = Arrays.copyOf(metaParams, metaParams.length, MetaParam[].class);
         }
         length = (int) Stream.of(metas).filter(Objects::nonNull).count();
      } else {
         List<MetaParam<?>> notFound = new ArrayList<>(metaParams.length);
         for (MetaParam newMeta : metaParams) {
            updateExisting(newMeta, notFound);
         }

         if (!notFound.isEmpty()) {
            MetaParam<?>[] newMetas = Arrays.copyOf(metas, metas.length + notFound.size());
            int i = metas.length;
            for (MetaParam<?> meta : notFound) {
               newMetas[i++] = meta;
            }
            metas = newMetas;
         }
      }
      assert checkLength();
   }

   private void updateExisting(MetaParam newMeta, List<MetaParam<?>> notFound) {
      int hole = -1;
      for (int i = 0; i < metas.length; i++) {
         MetaParam<?> m = metas[i];
         if (m == null) {
            hole = i;
         } else if (m.getClass().isAssignableFrom(newMeta.getClass())) {
            metas[i] = newMeta;
            return;
         }
      }
      ++length;
      if (hole < 0) {
         notFound.add(newMeta);
      } else {
         metas[hole] = newMeta;
      }
   }

   public <T extends MetaParam> void remove(Class<T> type) {
      for (int i = 0; i < metas.length; ++i) {
         MetaParam<?> m = metas[i];
         if (m != null && m.getClass().isAssignableFrom(type)) {
            metas[i] = null;
            --length;
            assert checkLength();
            return;
         }
      }
   }

   public <T extends MetaParam> void replace(Class<T> type, Function<T, T> f) {
      int hole = -1;
      for (int i = 0; i < metas.length; ++i) {
         MetaParam<?> m = metas[i];
         if (m == null) {
            hole = i;
         } else if (m.getClass().isAssignableFrom(type)) {
            T newMeta = f.apply((T) m);
            assert newMeta == null || type.isInstance(newMeta);
            if (newMeta == null) {
               --length;
            }
            metas[i] = newMeta;
            assert checkLength();
            return;
         }
      }
      T newMeta = f.apply(null);
      if (newMeta == null) {
         assert checkLength();
         return;
      } else if (hole < 0) {
         ++length;
         MetaParam<?>[] newMetas = Arrays.copyOf(metas, metas.length + 1);
         newMetas[newMetas.length - 1] = newMeta;
         metas = newMetas;
      } else {
         ++length;
         metas[hole] = newMeta;
      }
      assert checkLength();
   }

   @Override
   public String toString() {
      return "MetaParams{length=" + length + ", metas=" + Arrays.toString(metas) + '}';
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
      metas = filterDuplicates(metas);
      return new MetaParams(metas, metas.length);
   }

   static MetaParams of(MetaParam meta) {
      assert meta != null;
      return new MetaParams(new MetaParam[]{meta}, 1);
   }

   static MetaParams empty() {
      return new MetaParams(EMPTY_ARRAY, 0);
   }

   private static MetaParam[] filterDuplicates(MetaParam... metas) {
      Map<Class<?>, MetaParam<?>> all = new HashMap<>();
      for (MetaParam meta : metas) {
         if (meta == null) continue;
         all.put(meta.getClass(), meta);
      }

      return all.values().toArray(new MetaParam[all.size()]);
   }

   void merge(MetaParams other) {
      Map<Class<?>, MetaParam<?>> all = new HashMap<>();
      // Add other's metadata first, because we don't want to override those already present
      for (MetaParam meta : other.metas) {
         if (meta == null) continue;
         all.put(meta.getClass(), meta);
      }
      for (MetaParam meta : metas) {
         if (meta == null) continue;
         all.put(meta.getClass(), meta);
      }


      metas = all.values().toArray(new MetaParam[all.size()]);
   }

   @Override
   public Iterator<MetaParam<?>> iterator() {
      return new It();
   }

   @Override
   public Spliterator<MetaParam<?>> spliterator() {
      return Spliterators.spliterator(iterator(), length, Spliterator.DISTINCT | Spliterator.NONNULL);
   }

   public static MetaParams readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      int length = input.readInt();
      MetaParam[] metas = new MetaParam[length];
      for (int i = 0; i < length; i++)
         metas[i] = (MetaParam) input.readObject();

      return new MetaParams(metas, metas.length);
   }

   public static void writeTo(ObjectOutput output, MetaParams params) throws IOException {
      output.writeInt(params.size());
      for (MetaParam meta : params) output.writeObject(meta);
   }

   private class It implements Iterator<MetaParam<?>> {
      private int i = 0;

      public It() {
         skipNulls();
      }

      private void skipNulls() {
         while (i < metas.length && metas[i] == null) ++i;
      }

      @Override
      public boolean hasNext() {
         return i < metas.length;
      }

      @Override
      public MetaParam<?> next() {
         if (i >= metas.length) {
            throw new NoSuchElementException();
         }
         MetaParam<?> meta = metas[i++];
         skipNulls();
         return meta;
      }
   }
}
