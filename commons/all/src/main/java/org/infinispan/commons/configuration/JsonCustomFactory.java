package org.infinispan.commons.configuration;

import static org.infinispan.commons.configuration.Json.factory;

import java.util.Collection;
import java.util.Map;

import org.infinispan.commons.dataconversion.MediaType;

/**
 * Custom {@link Json.Factory} to handle cache attribute values.
 *
 * @since 10.0
 */
public class JsonCustomFactory extends Json.DefaultFactory {

   private final JsonWriter writer = new JsonWriter();

   @Override
   public Json make(Object anything) {
      if (anything == null)
         return Json.topnull;
      else if (anything instanceof Json)
         return (Json) anything;
      else if (anything instanceof String)
         return factory().string((String) anything);
      else if (anything instanceof Collection<?>) {
         Json L = array();
         for (Object x : (Collection<?>) anything)
            L.add(factory().make(x));
         return L;
      } else if (anything instanceof Map<?, ?>) {
         Json O = object();
         for (Map.Entry<?, ?> x : ((Map<?, ?>) anything).entrySet())
            O.set(x.getKey().toString(), factory().make(x.getValue()));
         return O;
      } else if (anything instanceof Boolean)
         return factory().bool((Boolean) anything);
      else if (anything instanceof Number)
         return factory().number((Number) anything);
      else if (anything instanceof Enum) {
         return factory().string(anything.toString());
      } else if (anything instanceof ConfigurationInfo) {
         Json object = object();
         writer.writeElement(object, (ConfigurationInfo) anything, false);
         return object;
      } else if (anything instanceof Class<?>) {
         return factory().string(((Class<?>) anything).getName());
      } else if (anything instanceof MediaType) {
         return factory().string(anything.toString());
      } else if (anything.getClass().isArray()) {
         Class<?> comp = anything.getClass().getComponentType();
         if (!comp.isPrimitive())
            return Json.array((Object[]) anything);
         Json A = array();
         if (boolean.class == comp)
            for (boolean b : (boolean[]) anything) A.add(b);
         else if (byte.class == comp)
            for (byte b : (byte[]) anything) A.add(b);
         else if (char.class == comp)
            for (char b : (char[]) anything) A.add(b);
         else if (short.class == comp)
            for (short b : (short[]) anything) A.add(b);
         else if (int.class == comp)
            for (int b : (int[]) anything) A.add(b);
         else if (long.class == comp)
            for (long b : (long[]) anything) A.add(b);
         else if (float.class == comp)
            for (float b : (float[]) anything) A.add(b);
         else if (double.class == comp)
            for (double b : (double[]) anything) A.add(b);
         return A;
      } else
         return make(anything.getClass().getName());
   }
}
