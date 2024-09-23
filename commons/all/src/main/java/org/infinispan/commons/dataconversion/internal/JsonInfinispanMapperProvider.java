package org.infinispan.commons.dataconversion.internal;

import java.util.ArrayList;

import org.infinispan.commons.dataconversion.internal.Json.NumberJson;
import org.infinispan.commons.dataconversion.internal.Json.StringJson;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.TypeRef;
import com.jayway.jsonpath.spi.mapper.MappingException;
import com.jayway.jsonpath.spi.mapper.MappingProvider;
public class JsonInfinispanMapperProvider implements MappingProvider {


   @SuppressWarnings("unchecked")
   @Override
    public <T> T map(Object source, Class<T> targetType, Configuration configuration) {
        if(source == null){
            return null;
        }
        if (targetType.isAssignableFrom(source.getClass())) {
            return (T) source;
        }
        try {
            if(!configuration.jsonProvider().isMap(source) && !configuration.jsonProvider().isArray(source)){
               if (source instanceof StringJson s) {
                  return (T) map(s, targetType);
               }
               if (source instanceof NumberJson n) {
                  return (T) map(n, targetType);
               }
               return (T) map(source);
            }
            if (configuration.jsonProvider().isArray(source) && targetType.isAssignableFrom(ArrayList.class)) {
               // isArray == true only for JsonArray here
               return (T) ((Json)source).asList();
            }

            if (targetType.isAssignableFrom(String.class)) {
               return (T) configuration.jsonProvider().toJson(source);
            }
            throw new MappingException("no mapping 2");
            //return (T) JSONValue.parse(s, targetType);
        } catch (Exception e) {
            throw new MappingException(e);
        }
    }

    public Object map(StringJson source, Class<?> tt) {
      if (tt.isAssignableFrom(String.class)) {
         return source.asString();
      }
      throw new MappingException("Cannot map StringJson to "+tt.getName());
    }

    public Object map(NumberJson source, Class<?> tt) {
      if (tt.isAssignableFrom(Integer.class) || tt.isAssignableFrom(int.class)) {
         return source.asInteger();
      }

      if (tt.isAssignableFrom(Long.class) || tt.isAssignableFrom(long.class)) {
         return source.asLong();
      }

      if (tt.isAssignableFrom(Float.class) || tt.isAssignableFrom(float.class)) {
         return source.asFloat();
      }

      if (tt.isAssignableFrom(Double.class) || tt.isAssignableFrom(double.class)) {
         return source.asDouble();
      }

      throw new MappingException("Cannot map NumberJson to "+tt.getName());
    }

    public Object map(Object source) {
      throw new UnsupportedOperationException("Json-smart provider does not support TypeRef! Use a Jackson or Gson based provider");
  }

    @Override
    public <T> T map(Object source, TypeRef<T> targetType, Configuration configuration) {
        throw new UnsupportedOperationException("JsonInfinispanMapperProvider provider does not support TypeRef! Use a Jackson or Gson based provider");
    }

}
