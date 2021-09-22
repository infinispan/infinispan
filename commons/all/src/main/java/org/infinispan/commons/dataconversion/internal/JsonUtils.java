package org.infinispan.commons.dataconversion.internal;

import java.util.Collection;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * Utility function for {@link Json}
 *
 * @author Pedro Ruivo
 * @since 13.0
 */
public final class JsonUtils {

   private JsonUtils() {
   }

   public static <T> Json createJsonArray(Collection<T> collection) {
      return createJsonArray(collection, Json::make);
   }

   public static <T> Json createJsonArray(Stream<T> stream) {
      return createJsonArray(stream, Json::make);
   }

   public static <T> Json createJsonArray(Collection<T> collection, Function<T, Json> jsonFactory) {
      return createJsonArray(collection.stream(), jsonFactory);
   }

   public static <T> Json createJsonArray(Stream<T> stream, Function<T, Json> jsonFactory) {
      return stream.map(jsonFactory).collect(new JsonArrayCollector());
   }

}
