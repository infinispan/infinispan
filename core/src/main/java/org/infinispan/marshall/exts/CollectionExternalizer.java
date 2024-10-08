package org.infinispan.marshall.exts;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.AbstractMap;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.commons.util.FastCopyHashMap;
import org.infinispan.commons.util.Util;
import org.infinispan.distribution.util.ReadOnlySegmentAwareCollection;
import org.infinispan.marshall.core.Ids;

public class CollectionExternalizer implements AdvancedExternalizer<Collection> {

   private static final int ARRAY_LIST = 0;
   private static final int LINKED_LIST = 1;
   private static final int SINGLETON_LIST = 2;
   private static final int EMPTY_LIST = 3;
   private static final int HASH_SET = 4;
   private static final int TREE_SET = 5;
   private static final int SINGLETON_SET = 6;
   private static final int SYNCHRONIZED_SET = 7;
   private static final int ARRAY_DEQUE = 8;
   private static final int READ_ONLY_SEGMENT_AWARE_COLLECTION = 9;
   private static final int ENTRY_SET = 10;
   private static final int EMPTY_SET = 11;
   private static final int SYNCHRONIZED_LIST = 12;

   private final Map<Class<?>, Integer> numbers = new HashMap<>(16);

   public CollectionExternalizer() {
      numbers.put(ArrayList.class, ARRAY_LIST);
      numbers.put(getPrivateArrayListClass(), ARRAY_LIST);
      numbers.put(getPrivateArrayListSubListClass(), ARRAY_LIST);
      numbers.put(getPrivateAbstractListRandomAccessSubListClass(), ARRAY_LIST);
      numbers.put(getPrivateUnmodifiableListClass(), ARRAY_LIST);
      numbers.put(getPrivateSynchronizedListClass(), SYNCHRONIZED_LIST);
      numbers.put(getPrivateImmutableList12Class(), ARRAY_LIST);
      numbers.put(getPrivateImmutableListNClass(), ARRAY_LIST);
      numbers.put(LinkedList.class, LINKED_LIST);
      numbers.put(getPrivateSingletonListClass(), SINGLETON_LIST);
      numbers.put(getPrivateEmptyListClass(), EMPTY_LIST);
      numbers.put(getPrivateEmptySetClass(), EMPTY_SET);
      numbers.put(ArrayDeque.class, ARRAY_DEQUE);
      numbers.put(HashSet.class, HASH_SET);
      numbers.put(TreeSet.class, TREE_SET);
      numbers.put(getPrivateSingletonSetClass(), SINGLETON_SET);
      numbers.put(getPrivateSynchronizedSetClass(), SYNCHRONIZED_SET);
      numbers.put(getPrivateUnmodifiableSetClass(), HASH_SET);
      numbers.put(ReadOnlySegmentAwareCollection.class, READ_ONLY_SEGMENT_AWARE_COLLECTION);
      numbers.put(FastCopyHashMap.KeySet.class, HASH_SET);
      numbers.put(FastCopyHashMap.Values.class, ARRAY_LIST);
      numbers.put(FastCopyHashMap.EntrySet.class, ENTRY_SET);
   }

   @Override
   public void writeObject(ObjectOutput output, Collection collection) throws IOException {
      int number = numbers.getOrDefault(collection.getClass(), -1);
      output.writeByte(number);
      switch (number) {
         case ARRAY_LIST:
         case LINKED_LIST:
         case HASH_SET:
         case SYNCHRONIZED_SET:
         case SYNCHRONIZED_LIST:
         case ARRAY_DEQUE:
         case READ_ONLY_SEGMENT_AWARE_COLLECTION:
            MarshallUtil.marshallCollection(collection, output);
            break;
         case SINGLETON_LIST:
            output.writeObject(((List) collection).get(0));
            break;
         case SINGLETON_SET:
            output.writeObject(collection.iterator().next());
            break;
         case TREE_SET:
            output.writeObject(((TreeSet) collection).comparator());
            MarshallUtil.marshallCollection(collection, output);
            break;
         case ENTRY_SET:
            MarshallUtil.marshallCollection(collection, output, (out, element) -> {
               Map.Entry entry = (Map.Entry) element;
               out.writeObject(entry.getKey());
               out.writeObject(entry.getValue());
            });
            break;
      }
   }

   @Override
   public Collection readObject(ObjectInput input) throws IOException, ClassNotFoundException {
      int magicNumber = input.readUnsignedByte();
      return switch (magicNumber) {
         case ARRAY_LIST, READ_ONLY_SEGMENT_AWARE_COLLECTION -> MarshallUtil.unmarshallCollection(input, ArrayList::new);
         case LINKED_LIST -> MarshallUtil.unmarshallCollectionUnbounded(input, LinkedList::new);
         case SINGLETON_LIST -> Collections.singletonList(input.readObject());
         case SYNCHRONIZED_LIST -> Collections.synchronizedList(MarshallUtil.unmarshallCollection(input, ArrayList::new));
         case EMPTY_LIST -> Collections.emptyList();
         case HASH_SET -> MarshallUtil.unmarshallCollection(input, s -> new HashSet<>());
         case TREE_SET -> {
            Comparator<Object> comparator = (Comparator<Object>) input.readObject();
            yield MarshallUtil.unmarshallCollection(input, s -> new TreeSet<>(comparator));
         }
         case SINGLETON_SET -> Collections.singleton(input.readObject());
         case SYNCHRONIZED_SET -> Collections.synchronizedSet(
               MarshallUtil.unmarshallCollection(input, s -> new HashSet<>()));
         case ARRAY_DEQUE -> MarshallUtil.unmarshallCollection(input, ArrayDeque::new);
         case ENTRY_SET -> MarshallUtil.<Map.Entry, Set<Map.Entry>>unmarshallCollection(input, s -> new HashSet(),
               in -> new AbstractMap.SimpleEntry(in.readObject(), in.readObject()));
         case EMPTY_SET -> Collections.emptySet();
         default -> throw new IllegalStateException("Unknown Set type: " + magicNumber);
      };
   }

   @Override
   public Integer getId() {
      return Ids.COLLECTIONS;
   }

   @Override
   public Set<Class<? extends Collection>> getTypeClasses() {
      Set<Class<? extends Collection>> typeClasses = Util.asSet(ArrayList.class, LinkedList.class,
            HashSet.class, TreeSet.class,
            ArrayDeque.class,
            ReadOnlySegmentAwareCollection.class,
            FastCopyHashMap.KeySet.class, FastCopyHashMap.Values.class, FastCopyHashMap.EntrySet.class);
      typeClasses.addAll(getSupportedPrivateClasses());
      return typeClasses;
   }

   /**
    * Returns an immutable Set that contains all of the private classes (e.g. java.util.Collections$EmptyList) that
    * are supported by this Externalizer. This method is to be used by external sources if these private classes
    * need additional processing to be available.
    * @return immutable set of the private classes
    */
   public static Set<Class<Collection>> getSupportedPrivateClasses() {
      return Set.of(getPrivateArrayListClass(),
            getPrivateArrayListSubListClass(),
            getPrivateAbstractListRandomAccessSubListClass(),
            getPrivateSynchronizedListClass(),
            getPrivateUnmodifiableListClass(),
            getPrivateEmptyListClass(),
            getPrivateEmptySetClass(),
            getPrivateSingletonListClass(),
            getPrivateSingletonSetClass(),
            getPrivateSynchronizedSetClass(),
            getPrivateUnmodifiableSetClass(),
            getPrivateImmutableList12Class(),
            getPrivateImmutableListNClass()
      );
   }

   private static Class<Collection> getPrivateArrayListClass() {
      return getCollectionClass("java.util.Arrays$ArrayList");
   }

   private static Class<Collection> getPrivateArrayListSubListClass() {
      return getCollectionClass("java.util.ArrayList$SubList");
   }

   private static Class<Collection> getPrivateAbstractListRandomAccessSubListClass() {
      return getCollectionClass("java.util.AbstractList$RandomAccessSubList");
   }

   public static Class<Collection> getPrivateSynchronizedListClass() {
      return getCollectionClass("java.util.Collections$SynchronizedRandomAccessList");
   }

   private static Class<Collection> getPrivateUnmodifiableListClass() {
      return getCollectionClass("java.util.Collections$UnmodifiableRandomAccessList");
   }

   private static Class<Collection> getPrivateEmptyListClass() {
      return getCollectionClass("java.util.Collections$EmptyList");
   }

   private static Class<Collection> getPrivateEmptySetClass() {
      return getCollectionClass("java.util.Collections$EmptySet");
   }

   private static Class<Collection> getPrivateSingletonListClass() {
      return getCollectionClass("java.util.Collections$SingletonList");
   }

   public static Class<Collection> getPrivateSingletonSetClass() {
      return getCollectionClass("java.util.Collections$SingletonSet");
   }

   public static Class<Collection> getPrivateSynchronizedSetClass() {
      return getCollectionClass("java.util.Collections$SynchronizedSet");
   }

   private static Class<Collection> getPrivateUnmodifiableSetClass() {
      return getCollectionClass("java.util.Collections$UnmodifiableSet");
   }

   private static Class<Collection> getPrivateImmutableList12Class() {
      return getCollectionClass("java.util.ImmutableCollections$List12");
   }

   private static Class<Collection> getPrivateImmutableListNClass() {
      return getCollectionClass("java.util.ImmutableCollections$ListN");
   }

   private static Class<Collection> getCollectionClass(String className) {
      return Util.loadClass(className, Collection.class.getClassLoader());
   }



}
