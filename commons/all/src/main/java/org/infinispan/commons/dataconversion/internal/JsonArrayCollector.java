package org.infinispan.commons.dataconversion.internal;

import java.util.Collections;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

/**
 * A {@link Collector} implementation that create a {@link Json} array.
 *
 * @author Pedro Ruivo
 * @since 13.0
 */
public class JsonArrayCollector implements Collector<Json, JsonArrayCollector, Json> {

   private final Json array;

   public JsonArrayCollector() {
      this.array = Json.array();
   }

   @Override
   public Supplier<JsonArrayCollector> supplier() {
      return () -> this;
   }

   @Override
   public BiConsumer<JsonArrayCollector, Json> accumulator() {
      return JsonArrayCollector::add;
   }

   @Override
   public BinaryOperator<JsonArrayCollector> combiner() {
      return JsonArrayCollector::combine;
   }

   @Override
   public Function<JsonArrayCollector, Json> finisher() {
      return JsonArrayCollector::getArray;
   }

   @Override
   public Set<Characteristics> characteristics() {
      return Collections.emptySet();
   }

   public void add(Json json) {
      this.array.add(json);
   }

   public JsonArrayCollector combine(JsonArrayCollector collector) {
      collector.array.asJsonList().forEach(this::add);
      return this;
   }

   public Json getArray() {
      return array;
   }
}
