package org.infinispan.marshall.persistence.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.infinispan.commons.util.FastCopyHashMap;
import org.infinispan.commons.util.Util;
import org.infinispan.protostream.EnumMarshaller;
import org.infinispan.protostream.MessageMarshaller;
import org.infinispan.util.KeyValuePair;

final class MapMarshaller implements MessageMarshaller<Map> {

   private static final Map<Class<?>, Type> typeMap = new HashMap<>();

   static {
      typeMap.put(getPrivateEmptyMapClass(), Type.COLLECTIONS_EMPTY);
      typeMap.put(getPrivateSingletonMapClass(), Type.COLLECTIONS_SINGLETON);
      typeMap.put(ConcurrentHashMap.class, Type.CONCURRENT);
      typeMap.put(FastCopyHashMap.class, Type.FASTCOPY);
      typeMap.put(HashMap.class, Type.HASH);
      typeMap.put(TreeMap.class, Type.TREE);
   }

   // Optimisation for known map types, if no type is specified then hash is assumed
   enum Type {
      COLLECTIONS_EMPTY(0),
      COLLECTIONS_SINGLETON(1),
      CONCURRENT(2),
      CUSTOM(3),
      FASTCOPY(4),
      HASH(5),
      TREE(6);

      final int index;

      Type(int index) {
         this.index = index;
      }

      static Type get(int index) {
         for (Type type : Type.values())
            if (type.index == index)
               return type;
         return null;
      }
   }

   @Override
   public Map readFrom(ProtoStreamReader reader) throws IOException {
      Type type = reader.readEnum("type", Type.class);
      Supplier<Map> mapSupplier = HashMap::new;
      switch (type) {
         case TREE:
            mapSupplier = TreeMap::new;
            break;
         case FASTCOPY:
            mapSupplier = FastCopyHashMap::new;
            break;
         case CONCURRENT:
            mapSupplier = ConcurrentHashMap::new;
            break;
         case COLLECTIONS_EMPTY:
            return Collections.emptyMap();
         case COLLECTIONS_SINGLETON:
            KeyValuePair kvp = reader.readArray("entry", KeyValuePair.class)[0];
            return Collections.singletonMap(kvp.getKey(), kvp.getValue());
         case CUSTOM:
            String implementation = reader.readString("implementation");
            mapSupplier = () -> {
               try {
                  return (Map) Class.forName(implementation).newInstance();
               } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
                  throw new IllegalStateException(e);
               }
            };
      }
      return reader
            .readCollection("entry", new ArrayList<>(), KeyValuePair.class)
            .stream()
            .collect(Collectors.toMap(KeyValuePair::getKey, KeyValuePair::getValue, (o, n) -> n, mapSupplier));
   }

   @Override
   public void writeTo(ProtoStreamWriter writer, Map map) throws IOException {
      Type type = typeMap.get(map.getClass());
      if (type != null) {
         // No need to write HASH as it's the default
         if (type != Type.HASH) {
            writer.writeEnum("type", type);
         }
      } else {
         writer.writeString("implementation", map.getClass().getName());
      }

      if (!map.isEmpty()) {
         List<KeyValuePair> kvp = ((Set<Map.Entry>) map.entrySet()).stream()
               .map(e -> new KeyValuePair<>(e.getKey(), e.getValue()))
               .collect(Collectors.toList());

         writer.writeCollection("entry", kvp, KeyValuePair.class);
      }
   }

   @Override
   public Class<? extends Map> getJavaClass() {
      return Map.class;
   }

   @Override
   public String getTypeName() {
      return "persistence.Map";
   }

   static class TypeMarshaller implements EnumMarshaller<Type> {
      @Override
      public Type decode(int enumValue) {
         return Type.get(enumValue);
      }

      @Override
      public int encode(Type metadataType) throws IllegalArgumentException {
         return metadataType.index;
      }

      @Override
      public Class<Type> getJavaClass() {
         return Type.class;
      }

      @Override
      public String getTypeName() {
         return "persistence.Map.Type";
      }
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
