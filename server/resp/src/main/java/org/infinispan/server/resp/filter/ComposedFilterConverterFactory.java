package org.infinispan.server.resp.filter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.infinispan.filter.KeyValueFilterConverter;
import org.infinispan.filter.ParamKeyValueFilterConverterFactory;

/**
 * Creates a {@link ComposedFilterConverter}.
 * <p>
 * During the allocation of the filter, the parameters must be a <code>byte[][]</code> and have the expected format. Only
 * an exhaustive list of filters are accepted to be composed. Which includes:
 * <ul>
 *    <li>{@link GlobMatchFilterConverter};
 *    <li>{@link RespTypeFilterConverter};
 * </ul>
 * <p>
 * To add more filters, the {@link #clazzToByte(Class)} needs to be updated, along with the allocation to compose the
 * filters.
 *
 * @since 15.0
 */
public class ComposedFilterConverterFactory implements ParamKeyValueFilterConverterFactory<byte[], Object, Object> {
   private static final byte GLOB_FILTER = 0;
   private static final byte TYPE_FILTER = 1;

   @Override
   public KeyValueFilterConverter<byte[], Object, Object> getFilterConverter(Object[] params) {
      assert (params.length & 1) == 0 : "Should have an even number of parameters.";

      List<KeyValueFilterConverter<byte[], Object, Object>> filterConverters = new ArrayList<>();
      for (int i = 0; i < params.length;) {
         byte[] type = (byte[]) params[i++];
         byte[] arguments = (byte[]) params[i++];

         switch (type[0]) {
            case GLOB_FILTER:
               KeyValueFilterConverter<byte[], ?, ?> filter = GlobMatchFilterConverterFactory.create(arguments, false);
               filterConverters.add((KeyValueFilterConverter<byte[], Object, Object>) filter);
               break;
            case TYPE_FILTER:
               filterConverters.add(RespTypeFilterConverterFactory.create(arguments));
               break;
            default:
               throw new IllegalArgumentException("Unknown filter with type: " + type[0]);
         }
      }
      return new ComposedFilterConverter<>(filterConverters);
   }

   @Override
   public boolean binaryParam() {
      return true;
   }

   public static Map.Entry<Class<?>, List<byte[]>> convertFiltersFormat(Map<Class<?>, List<byte[]>> filters) {
      if (filters.size() == 1) {
         return filters.entrySet().stream().findFirst().orElse(null);
      }

      List<byte[]> arguments = new ArrayList<>(4);
      for (Map.Entry<Class<?>, List<byte[]>> e : filters.entrySet()) {
         arguments.add(clazzToByte(e.getKey()));
         arguments.addAll(e.getValue());
      }

      return Map.entry(ComposedFilterConverterFactory.class, arguments);
   }

   private static byte[] clazzToByte(Class<?> clazz) {
      if (clazz == GlobMatchFilterConverterFactory.class) {
         return new byte[] { GLOB_FILTER };
      }

      if (clazz == RespTypeFilterConverterFactory.class) {
         return new byte[] { TYPE_FILTER };
      }

      throw new IllegalArgumentException("Unknown filter: " + clazz.getName());
   }
}
