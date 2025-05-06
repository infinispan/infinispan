package org.infinispan.tools.store.migrator.marshaller.common;

import java.io.IOException;
import java.io.ObjectInput;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.commons.util.Util;
import org.infinispan.distribution.util.ReadOnlySegmentAwareMap;

public class MapExternalizer extends AbstractMigratorExternalizer<Map> {
   private static final int HASHMAP = 0;
   private static final int TREEMAP = 1;
   private static final int FASTCOPYHASHMAP = 2;
   private static final int EQUIVALENTHASHMAP = 3;
   private static final int CONCURRENTHASHMAP = 4;
   // 5 reserved for the removed EntryVersionsMap
   private static final int SINGLETONMAP = 6;
   private static final int EMPTYMAP = 7;

   public MapExternalizer() {
      super(getSupportedPrivateClasses(), Ids.MAPS);
   }

   @Override
   public Map readObject(ObjectInput input) throws IOException, ClassNotFoundException {
      int magicNumber = input.readUnsignedByte();
      return switch (magicNumber) {
         case HASHMAP -> MarshallUtil.unmarshallMap(input, HashMap::new);
         case TREEMAP -> MarshallUtil.unmarshallMap(input, size -> new TreeMap<>());
         case CONCURRENTHASHMAP -> MarshallUtil.unmarshallMap(input, ConcurrentHashMap::new);
         case SINGLETONMAP -> Collections.singletonMap(input.readObject(), input.readObject());
         case EMPTYMAP -> Collections.emptyMap();
         default -> throw new IllegalStateException("Unknown Map type: " + magicNumber);
      };
   }

   /**
    * Returns an immutable Set that contains all of the private classes (e.g. java.util.Collections$EmptyMap) that
    * are supported by this Externalizer. This method is to be used by external sources if these private classes
    * need additional processing to be available.
    * @return immutable set of the private classes
    */
   public static Set<Class<? extends Map>> getSupportedPrivateClasses() {
      return Set.of(
            HashMap.class, TreeMap.class,
            ReadOnlySegmentAwareMap.class, ConcurrentHashMap.class,
            getPrivateSingletonMapClass(),
            getPrivateEmptyMapClass(),
            getPrivateImmutableMap1Class(),
            getPrivateImmutableMapNClass()
      );
   }

   private static Class<? extends Map> getPrivateSingletonMapClass() {
      return getMapClass("java.util.Collections$SingletonMap");
   }

   private static Class<? extends Map> getPrivateEmptyMapClass() {
      return getMapClass("java.util.Collections$EmptyMap");
   }

   private static Class<? extends Map> getPrivateImmutableMap1Class() {
      return getMapClass("java.util.ImmutableCollections$Map1");
   }

   private static Class<? extends Map> getPrivateImmutableMapNClass() {
      return getMapClass("java.util.ImmutableCollections$MapN");
   }

   private static Class<? extends Map> getMapClass(String className) {
      return Util.loadClass(className, Map.class.getClassLoader());
   }
}
