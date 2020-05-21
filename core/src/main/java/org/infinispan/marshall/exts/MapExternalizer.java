package org.infinispan.marshall.exts;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.commons.util.FastCopyHashMap;
import org.infinispan.commons.util.Util;
import org.infinispan.distribution.util.ReadOnlySegmentAwareMap;
import org.infinispan.marshall.core.Ids;

/**
 * Map externalizer for all map implementations except immutable maps and singleton maps, i.e. FastCopyHashMap, HashMap,
 * TreeMap.
 *
 * @author Galder Zamarreño
 * @since 4.0
 *
 */
public class MapExternalizer extends AbstractExternalizer<Map> {
   private static final int HASHMAP = 0;
   private static final int TREEMAP = 1;
   private static final int FASTCOPYHASHMAP = 2;
   private static final int EQUIVALENTHASHMAP = 3;
   private static final int CONCURRENTHASHMAP = 4;
   // 5 reserved for the removed EntryVersionsMap
   private static final int SINGLETONMAP = 6;
   private static final int EMPTYMAP = 7;
   private final Map<Class<?>, Integer> numbers = new HashMap<>(8);

   public MapExternalizer() {
      numbers.put(HashMap.class, HASHMAP);
      numbers.put(ReadOnlySegmentAwareMap.class, HASHMAP);
      numbers.put(TreeMap.class, TREEMAP);
      numbers.put(FastCopyHashMap.class, FASTCOPYHASHMAP);
      numbers.put(ConcurrentHashMap.class, CONCURRENTHASHMAP);
      numbers.put(getPrivateSingletonMapClass(), SINGLETONMAP);
      numbers.put(getPrivateEmptyMapClass(), EMPTYMAP);
   }

   @Override
   public void writeObject(ObjectOutput output, Map map) throws IOException {
      int number = numbers.getOrDefault(map.getClass(), -1);
      output.write(number);
      switch (number) {
         case HASHMAP:
         case TREEMAP:
         case CONCURRENTHASHMAP:
            MarshallUtil.marshallMap(map, output);
            break;
         case FASTCOPYHASHMAP:
            //copy the map to avoid ConcurrentModificationException
            MarshallUtil.marshallMap(((FastCopyHashMap<?, ?>) map).clone(), output);
            break;
         case SINGLETONMAP:
            Map.Entry singleton = (Map.Entry) map.entrySet().iterator().next();
            output.writeObject(singleton.getKey());
            output.writeObject(singleton.getValue());
            break;
         default:
            break;
      }
   }

   @Override
   public Map readObject(ObjectInput input) throws IOException, ClassNotFoundException {
      int magicNumber = input.readUnsignedByte();
      switch (magicNumber) {
         case HASHMAP:
            return MarshallUtil.unmarshallMap(input, HashMap::new);
         case TREEMAP:
            return MarshallUtil.unmarshallMap(input, size -> new TreeMap<>());
         case FASTCOPYHASHMAP:
            return MarshallUtil.unmarshallMap(input, FastCopyHashMap::new);
         case CONCURRENTHASHMAP:
            return MarshallUtil.unmarshallMap(input, ConcurrentHashMap::new);
         case SINGLETONMAP:
            return Collections.singletonMap(input.readObject(), input.readObject());
         case EMPTYMAP:
            return Collections.emptyMap();
         default:
            throw new IllegalStateException("Unknown Map type: " + magicNumber);
      }
   }

   @Override
   public Integer getId() {
      return Ids.MAPS;
   }

   @Override
   public Set<Class<? extends Map>> getTypeClasses() {
      Set<Class<? extends Map>> typeClasses = Util.asSet(
            HashMap.class, TreeMap.class, FastCopyHashMap.class,
            ReadOnlySegmentAwareMap.class, ConcurrentHashMap.class);
      typeClasses.addAll(getSupportedPrivateClasses());
      return typeClasses;
   }

   /**
    * Returns an immutable Set that contains all of the private classes (e.g. java.util.Collections$EmptyMap) that
    * are supported by this Externalizer. This method is to be used by external sources if these private classes
    * need additional processing to be available.
    * @return immutable set of the private classes
    */
   public static Set<Class<? extends Map>> getSupportedPrivateClasses() {
      Set<Class<? extends Map>> classNames = new HashSet<>(Arrays.asList(
            getPrivateSingletonMapClass(),
            getPrivateEmptyMapClass()
      ));
      return Collections.unmodifiableSet(classNames);
   }

   private static Class<? extends Map> getPrivateSingletonMapClass() {
      return getMapClass("java.util.Collections$SingletonMap");
   }

   private static Class<? extends Map> getPrivateEmptyMapClass() {
      return getMapClass("java.util.Collections$EmptyMap");
   }

   private static Class<? extends Map> getMapClass(String className) {
      return Util.loadClass(className, Map.class.getClassLoader());
   }

}
