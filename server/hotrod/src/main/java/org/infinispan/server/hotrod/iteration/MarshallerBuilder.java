package org.infinispan.server.hotrod.iteration;

import java.lang.reflect.InvocationTargetException;
import java.util.Optional;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.marshall.jboss.GenericJBossMarshaller;
import org.infinispan.filter.KeyValueFilterConverter;

/**
 * @author gustavonalle
 * @since 8.0
 */
class MarshallerBuilder {
   static <K, V, C> Class<?> toClass(IterationFilter<K, V, C> filter) {
      return filter.marshaller.map(Object::getClass).orElse(null);
   }

   static Marshaller fromClass(Optional<Class<Marshaller>> marshallerClass, Optional<KeyValueFilterConverter> filter) {
      return filter.flatMap(f -> marshallerClass.map(m -> {
         try {
            return m.getConstructor(ClassLoader.class);
         } catch (NoSuchMethodException e) {
            throw new CacheException(e);
         }
      }).map(c -> {
         try {
            return c.newInstance(f.getClass().getClassLoader());
         } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new CacheException(e);
         }
      })).orElseGet(() -> marshallerClass.map(c -> {
         try {
            return c.newInstance();
         } catch (InstantiationException | IllegalAccessException e) {
            throw new CacheException(e);
         }
      }).orElseGet(() -> genericFromInstance(filter)));
   }

   static Marshaller genericFromInstance(Optional<?> instance) {
      return new GenericJBossMarshaller(instance.map(i -> i.getClass().getClassLoader()).orElse(null));
   }
}
