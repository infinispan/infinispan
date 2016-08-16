package org.infinispan.marshall.exts;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import org.infinispan.commons.equivalence.Equivalence;
import org.infinispan.commons.equivalence.EquivalentHashMap;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.commons.util.FastCopyHashMap;
import org.infinispan.commons.util.Util;
import org.infinispan.container.versioning.EntryVersionsMap;
import org.infinispan.distribution.util.ReadOnlySegmentAwareMap;
import org.infinispan.marshall.core.Ids;
import org.jboss.marshalling.util.IdentityIntMap;

/**
 * Map externalizer for all map implementations except immutable maps and singleton maps, i.e. FastCopyHashMap, HashMap,
 * TreeMap.
 *
 * @author Galder Zamarreño
 * @since 4.0
 */
public class MapExternalizer extends AbstractExternalizer<Map> {
   private static final int HASHMAP = 0;
   private static final int TREEMAP = 1;
   private static final int FASTCOPYHASHMAP = 2;
   private static final int EQUIVALENTHASHMAP = 3;
   private static final int CONCURRENTHASHMAP = 4;
   private static final int ENTRYVERSIONMAP = 5;
   private static final int SINGLETONMAP = 6;
   private static final int EMPTYMAP = 7;
   private final IdentityIntMap<Class<?>> numbers = new IdentityIntMap<Class<?>>(9);

   public MapExternalizer() {
      numbers.put(HashMap.class, HASHMAP);
      numbers.put(ReadOnlySegmentAwareMap.class, HASHMAP);
      numbers.put(TreeMap.class, TREEMAP);
      numbers.put(FastCopyHashMap.class, FASTCOPYHASHMAP);
      numbers.put(EquivalentHashMap.class, EQUIVALENTHASHMAP);
      numbers.put(ConcurrentHashMap.class, CONCURRENTHASHMAP);
      numbers.put(EntryVersionsMap.class, ENTRYVERSIONMAP);
      numbers.put(getPrivateSingletonMapClass(), SINGLETONMAP);
      numbers.put(getPrivateEmptyMapClass(), EMPTYMAP);
   }

   @Override
   public void writeObject(ObjectOutput output, Map map) throws IOException {
      int number = numbers.get(map.getClass(), -1);
      output.write(number);
      switch (number) {
         case HASHMAP:
         case TREEMAP:
         case CONCURRENTHASHMAP:
         case ENTRYVERSIONMAP:
            MarshallUtil.marshallMap(map, output);
            break;
         case EQUIVALENTHASHMAP:
            EquivalentHashMap equivalentMap = (EquivalentHashMap) map;
            output.writeObject(equivalentMap.getKeyEquivalence());
            output.writeObject(equivalentMap.getValueEquivalence());
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
      Map subject = null;
      switch (magicNumber) {
         case HASHMAP:
            return MarshallUtil.unmarshallMap(input, HashMap::new);
         case TREEMAP:
            return MarshallUtil.unmarshallMap(input, size -> new TreeMap<>());
         case FASTCOPYHASHMAP:
            return MarshallUtil.unmarshallMap(input, FastCopyHashMap::new);
         case EQUIVALENTHASHMAP:
            Equivalence<Object> keyEq = (Equivalence<Object>) input.readObject();
            Equivalence<Object> valueEq = (Equivalence<Object>) input.readObject();
            return MarshallUtil.unmarshallMap(input, size -> new EquivalentHashMap<>(keyEq, valueEq));
         case CONCURRENTHASHMAP:
            return MarshallUtil.unmarshallMap(input, ConcurrentHashMap::new);
         case ENTRYVERSIONMAP:
            return MarshallUtil.unmarshallMap(input, EntryVersionsMap::new);
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
      return Util.<Class<? extends Map>>asSet(
            HashMap.class, TreeMap.class, FastCopyHashMap.class, EquivalentHashMap.class,
            ReadOnlySegmentAwareMap.class, ConcurrentHashMap.class,
            EntryVersionsMap.class, getPrivateSingletonMapClass(), getPrivateEmptyMapClass());
   }

   private static Class<Map> getPrivateSingletonMapClass() {
      return getMapClass("java.util.Collections$SingletonMap");
   }

   private static Class<Map> getPrivateEmptyMapClass() {
      return getMapClass("java.util.Collections$EmptyMap");
   }

   private static Class<Map> getMapClass(String className) {
      return Util.<Map>loadClass(className, Map.class.getClassLoader());
   }

}
