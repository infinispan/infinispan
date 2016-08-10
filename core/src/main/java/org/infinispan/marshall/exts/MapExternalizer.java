package org.infinispan.marshall.exts;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
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
   private final IdentityIntMap<Class<?>> numbers = new IdentityIntMap<Class<?>>(4);

   public MapExternalizer() {
      numbers.put(HashMap.class, HASHMAP);
      numbers.put(ReadOnlySegmentAwareMap.class, HASHMAP);
      numbers.put(TreeMap.class, TREEMAP);
      numbers.put(FastCopyHashMap.class, FASTCOPYHASHMAP);
      numbers.put(EquivalentHashMap.class, EQUIVALENTHASHMAP);
      numbers.put(ConcurrentHashMap.class, CONCURRENTHASHMAP);
   }

   @Override
   public void writeObject(ObjectOutput output, Map map) throws IOException {
      int number = numbers.get(map.getClass(), -1);
      output.write(number);
      switch (number) {
         case EQUIVALENTHASHMAP:
            EquivalentHashMap equivalentMap = (EquivalentHashMap) map;
            output.writeObject(equivalentMap.getKeyEquivalence());
            output.writeObject(equivalentMap.getValueEquivalence());
            break;
         case FASTCOPYHASHMAP:
            //copy the map to avoid ConcurrentModificationException
            MarshallUtil.marshallMap(((FastCopyHashMap<?, ?>) map).clone(), output);
            return;
         default:
            break;
      }
      MarshallUtil.marshallMap(map, output);
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
            ReadOnlySegmentAwareMap.class, ConcurrentHashMap.class);
   }
}
