package org.infinispan.functional;

import static org.infinispan.functional.FunctionalListenerAssertions.TestType.CREATE;
import static org.infinispan.functional.FunctionalListenerAssertions.TestType.MODIFY;
import static org.infinispan.functional.FunctionalListenerAssertions.TestType.REMOVE;
import static org.infinispan.functional.FunctionalListenerAssertions.TestType.WRITE;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import javax.cache.Cache;

import org.infinispan.functional.EntryView.ReadEntryView;
import org.infinispan.functional.decorators.FunctionalListeners;

public class FunctionalListenerAssertions<K, V> implements AutoCloseable {

   public enum TestType {
      CREATE, MODIFY, REMOVE, WRITE;

      private InternalTestType lambdaType() {
         switch (this) {
            case CREATE: return InternalTestType.LAMBDA_CREATE;
            case MODIFY: return InternalTestType.LAMBDA_MODIFY;
            case REMOVE: return InternalTestType.LAMBDA_REMOVE;
            case WRITE: return InternalTestType.LAMBDA_WRITE;
         }
         return null;
      }

      private InternalTestType listenerType() {
         switch (this) {
            case CREATE: return InternalTestType.LISTENER_CREATE;
            case MODIFY: return InternalTestType.LISTENER_MODIFY;
            case REMOVE: return InternalTestType.LISTENER_REMOVE;
            case WRITE: return InternalTestType.LISTENER_WRITE;
         }
         return null;
      }
   }

   private enum InternalTestType {
      LAMBDA_CREATE, LAMBDA_MODIFY, LAMBDA_REMOVE,
      LISTENER_CREATE, LISTENER_MODIFY, LISTENER_REMOVE,
      LAMBDA_WRITE, LISTENER_WRITE
   }

   final FunctionalListeners<K, V> listeners;
   final Runnable runnable;
   final List<TestEvent<V>> recorded = new ArrayList<>();
   final List<AutoCloseable> closeables = new ArrayList<>();

   @SuppressWarnings("unchecked")
   public static <K, V> FunctionalListenerAssertions<K, V> create(
         ConcurrentMap<K, V> map, Runnable r) {
      return new FunctionalListenerAssertions<>((FunctionalListeners<K, V>) map, r);
   }

   @SuppressWarnings("unchecked")
   public static <K, V> FunctionalListenerAssertions<K, V> create(
         Cache<K, V> map, Runnable r) {
      return new FunctionalListenerAssertions<>((FunctionalListeners<K, V>) map, r);
   }

   public static <K, V> FunctionalListenerAssertions<K, V> create(
         FunctionalListeners<K, V> listeners, Runnable r) {
      return new FunctionalListenerAssertions<>(listeners, r);
   }

   private FunctionalListenerAssertions(FunctionalListeners<K, V> listeners, Runnable runnable) {
      this.listeners = listeners;
      this.runnable = runnable;
      Listeners.ReadWriteListeners<K, V> rw = this.listeners.readWriteListeners();
      closeables.add(rw.onCreate(c -> recorded.add(
         TestEvent.create(InternalTestType.LAMBDA_CREATE, c.get()))));
      closeables.add(rw.onModify((b, a) -> recorded.add(
         TestEvent.create(InternalTestType.LAMBDA_MODIFY, a.get(), b.get()))));
      closeables.add(rw.onRemove(r -> recorded.add(
         TestEvent.create(InternalTestType.LAMBDA_REMOVE, null, r.get()))));
      closeables.add(rw.add(new Listeners.ReadWriteListeners.ReadWriteListener<K, V>() {
         @Override
         public void onCreate(ReadEntryView<K, V> created) {
            recorded.add(TestEvent.create(InternalTestType.LISTENER_CREATE, created.get()));
         }

         @Override
         public void onModify(ReadEntryView<K, V> before, ReadEntryView<K, V> after) {
            recorded.add(TestEvent.create(InternalTestType.LISTENER_MODIFY, after.get(), before.get()));
         }

         @Override
         public void onRemove(ReadEntryView<K, V> removed) {
            recorded.add(TestEvent.create(InternalTestType.LISTENER_REMOVE, null, removed.get()));
         }
      }));
      Listeners.WriteListeners<K, V> wo = this.listeners.writeOnlyListeners();
      closeables.add(wo.onWrite(w -> recorded.add(
         TestEvent.create(InternalTestType.LAMBDA_WRITE, w.find().orElse(null)))));
      closeables.add(wo.add(w -> recorded.add(
         TestEvent.create(InternalTestType.LISTENER_WRITE, w.find().orElse(null)))));
   }

   @Override
   public void close() {
      closeables.forEach(ac -> {
         try {
            ac.close();
         } catch (Exception e) {
            throw new AssertionError(e);
         }
      });
   }

   public void assertOrderedEvents(Collection<TestEvent<V>> expected) {
      runnable.run();
      assertEquals(expected, recorded);
   }

   public void assertUnorderedEvents(Collection<TestEvent<V>> expected) {
      runnable.run();
      assertEquals(recorded.toString(), expected.size(), recorded.size());
      expected.forEach(e -> assertTrue(String.format("Value %s not in %s", e, recorded),
         recorded.remove(e)));
      assertEquals(0, recorded.size());
   }

   private static <K, V> void withAssertions(ConcurrentMap<K, V> map, Runnable r,
         Consumer<FunctionalListenerAssertions<K, V>> c) {
      try(FunctionalListenerAssertions<K, V> a = FunctionalListenerAssertions.create(map, r)) {
         c.accept(a);
      }
   }

   private static <K, V> void withAssertions(Cache<K, V> map, Runnable r,
      Consumer<FunctionalListenerAssertions<K, V>> c) {
      try(FunctionalListenerAssertions<K, V> a = FunctionalListenerAssertions.create(map, r)) {
         c.accept(a);
      }
   }

   private static <K, V> void withAssertions(FunctionalListeners<K, V> listeners, Runnable r,
      Consumer<FunctionalListenerAssertions<K, V>> c) {
      try(FunctionalListenerAssertions<K, V> a = FunctionalListenerAssertions.create(listeners, r)) {
         c.accept(a);
      }
   }

   public static <K, V> void assertOrderedEvents(ConcurrentMap<K, V> map,
         Runnable r, Collection<TestEvent<V>> expected) {
      withAssertions(map, r, a -> a.assertOrderedEvents(expected));
   }

   public static <K, V> void assertOrderedEvents(Cache<K, V> cache,
         Runnable r, Collection<TestEvent<V>> expected) {
      withAssertions(cache, r, a -> a.assertOrderedEvents(expected));
   }

   public static <K, V> void assertOrderedEvents(FunctionalListeners<K, V> listeners,
         Runnable r, Collection<TestEvent<V>> expected) {
      withAssertions(listeners, r, a -> a.assertOrderedEvents(expected));
   }

   public static <K, V> void assertUnorderedEvents(ConcurrentMap<K, V> map,
         Runnable r, Collection<TestEvent<V>> expected) {
      withAssertions(map, r, a -> a.assertUnorderedEvents(expected));
   }

   public static <K, V> void assertUnorderedEvents(Cache<K, V> cache,
         Runnable r, Collection<TestEvent<V>> expected) {
      withAssertions(cache, r, a -> a.assertUnorderedEvents(expected));
   }

   public static <K, V> void assertUnorderedEvents(FunctionalListeners<K, V> listeners,
         Runnable r, Collection<TestEvent<V>> expected) {
      withAssertions(listeners, r, a -> a.assertUnorderedEvents(expected));
   }

   public static <K, V> void assertNoEvents(ConcurrentMap<K, V> map, Runnable r) {
      withAssertions(map, r, a -> a.assertOrderedEvents(new ArrayList<>()));
   }

   public static <K, V> void assertNoEvents(Cache<K, V> cache, Runnable r) {
      withAssertions(cache, r, a -> a.assertOrderedEvents(new ArrayList<>()));
   }

   public static <K, V> void assertNoEvents(FunctionalListeners<K, V> listeners, Runnable r) {
      withAssertions(listeners, r, a -> a.assertOrderedEvents(new ArrayList<>()));
   }

   public static Collection<TestEvent<String>> create(String... values) {
      List<TestEvent<String>> all = new ArrayList<>();
      for (String value : values) all.addAll(TestEvent.create(CREATE, value));
      for (String value : values) all.addAll(TestEvent.create(WRITE, value));
      return all;
   }

   public static Collection<TestEvent<String>> createModify(String createdValue, String modifiedValue) {
      List<TestEvent<String>> all = new ArrayList<>();
      all.addAll(TestEvent.create(CREATE, createdValue));
      all.addAll(TestEvent.create(WRITE, createdValue));
      all.addAll(TestEvent.create(MODIFY, modifiedValue, createdValue));
      all.addAll(TestEvent.create(WRITE, modifiedValue));
      return all;
   }

   public static Collection<TestEvent<String>> createRemove(String value) {
      List<TestEvent<String>> all = new ArrayList<>();
      all.addAll(TestEvent.create(CREATE, value));
      all.addAll(TestEvent.create(WRITE, value));
      all.addAll(TestEvent.create(REMOVE, null, value));
      all.addAll(TestEvent.create(WRITE, null));
      return all;
   }

   public static Collection<TestEvent<String>> createAllRemoveAll(String... values) {
      List<TestEvent<String>> all = new ArrayList<>();
      for (String s : values) {
         all.addAll(TestEvent.create(CREATE, s));
         all.addAll(TestEvent.create(WRITE, s));
      }
      for (String s : values) {
         all.addAll(TestEvent.create(REMOVE, null, s));
         all.addAll(TestEvent.create(WRITE, null));
      }
      return all;
   }

   public static Collection<TestEvent<String>> createThenRemove(String... values) {
      List<TestEvent<String>> all = new ArrayList<>();
      for (String s : values) {
         all.addAll(TestEvent.create(CREATE, s));
         all.addAll(TestEvent.create(WRITE, s));
         all.addAll(TestEvent.create(REMOVE, null, s));
         all.addAll(TestEvent.create(WRITE, null));
      }
      return all;
   }

   public static Collection<TestEvent<String>> write(String... values) {
      List<TestEvent<String>> all = new ArrayList<>();
      for (String value : values) all.addAll(TestEvent.create(WRITE, value));
      return all;
   }

   public static Collection<TestEvent<String>> writeModify(List<String> written, List<String> modified) {
      List<TestEvent<String>> all = new ArrayList<>();
      for (String value : written) all.addAll(TestEvent.create(WRITE, value));
      IntStream.range(0, modified.size()).forEach(i -> {
            all.addAll(TestEvent.create(MODIFY, modified.get(i), written.get(i)));
            all.addAll(TestEvent.create(WRITE, modified.get(i)));
         }
      );
      return all;
   }

   public static Collection<TestEvent<String>> createModifyRemove(String created, String modified) {
      List<TestEvent<String>> all = new ArrayList<>();
      all.addAll(TestEvent.create(CREATE, created));
      all.addAll(TestEvent.create(WRITE, created));
      all.addAll(TestEvent.create(MODIFY, modified, created));
      all.addAll(TestEvent.create(WRITE, modified));
      all.addAll(TestEvent.create(REMOVE, null, modified));
      all.addAll(TestEvent.create(WRITE, null));
      return all;
   }

   public static Collection<TestEvent<String>> createModifyRemove(List<String> created, List<String> modified) {
      List<TestEvent<String>> all = new ArrayList<>();
      created.forEach(s -> all.addAll(TestEvent.create(CREATE, s)));
      created.forEach(s -> all.addAll(TestEvent.create(WRITE, s)));
      modified.forEach(s -> all.addAll(TestEvent.create(WRITE, s)));
      modified.forEach(s -> all.addAll(TestEvent.create(REMOVE, null, s)));
      modified.forEach(s -> all.addAll(TestEvent.create(WRITE, null)));
      return all;
   }

   public static Collection<TestEvent<String>> writeValueNull(String... values) {
      List<TestEvent<String>> all = new ArrayList<>();
      for (String s : values) all.addAll(TestEvent.create(WRITE, s));
      for (String s : values) all.addAll(TestEvent.create(WRITE, null));
      return all;
   }

   public static Collection<TestEvent<String>> writeRemove(String... values) {
      List<TestEvent<String>> all = new ArrayList<>();
      for (String s : values) all.addAll(TestEvent.create(WRITE, s));
      for (String s : values) all.addAll(TestEvent.create(REMOVE, null, s));
      for (String s : values) all.addAll(TestEvent.create(WRITE, null));
      return all;
   }

   public static final class TestEvent<V> {
      final InternalTestType type;
      final Optional<V> prev;
      final V value;

      public static <V> Collection<TestEvent<V>> create(TestType type, V value) {
         return Arrays.asList(TestEvent.create(type.lambdaType(), value),
            TestEvent.create(type.listenerType(), value));
      }

      public static <V> Collection<TestEvent<V>> create(TestType type, V value, V prev) {
         return Arrays.asList(TestEvent.create(type.lambdaType(), value, prev),
            TestEvent.create(type.listenerType(), value, prev));
      }

      private static <V> TestEvent<V> create(InternalTestType type, V value) {
         return new TestEvent<>(type, Optional.empty(), value);
      }

      private static <V> TestEvent<V> create(InternalTestType type, V value, V prev) {
         return new TestEvent<>(type, Optional.of(prev), value);
      }

      private TestEvent(InternalTestType type, Optional<V> prev, V value) {
         this.type = type;
         this.prev = prev;
         this.value = value;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;

         TestEvent<?> testEvent = (TestEvent<?>) o;

         if (type != testEvent.type) return false;
         if (!prev.equals(testEvent.prev)) return false;
         return !(value != null ? !value.equals(testEvent.value) : testEvent.value != null);

      }

      @Override
      public int hashCode() {
         int result = type.hashCode();
         result = 31 * result + prev.hashCode();
         result = 31 * result + (value != null ? value.hashCode() : 0);
         return result;
      }

      @Override
      public String toString() {
         return "TestEvent{" +
            "type=" + type +
            ", prev=" + prev +
            ", value=" + value +
            '}';
      }
   }

}
