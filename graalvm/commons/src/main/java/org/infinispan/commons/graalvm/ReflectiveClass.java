package org.infinispan.commons.graalvm;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.graalvm.nativeimage.hosted.RuntimeReflection;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.dataconversion.internal.JsonSerialization;

public class ReflectiveClass implements JsonSerialization {

   final Class<?> clazz;
   final Constructor<?>[] constructors;
   final Field[] fields;
   final Method[] methods;

   public static ReflectiveClass of(Class<?> clazz) {
      return ReflectiveClass.of(clazz, false, false);
   }

   public static ReflectiveClass of(Class<?> clazz, boolean allFields, boolean allMethods) {
      Constructor<?>[] constructors = clazz.getDeclaredConstructors();
      Field[] fields = allFields ? getAllFields(clazz) : new Field[0];
      Method[] methods = allMethods ? getAllMethods(clazz) : new Method[0];
      return new ReflectiveClass(clazz, constructors, fields, methods);
   }

   private static Field[] getAllFields(Class<?> type) {
      List<Field> fields = new ArrayList<>();
      fields.addAll(Arrays.asList(type.getDeclaredFields()));
      return fields.toArray(new Field[0]);
   }

   private static Method[] getAllMethods(Class<?> type) {
      List<Method> methods = new ArrayList<>();
      methods.addAll(Arrays.asList(type.getDeclaredMethods()));
      return methods.toArray(new Method[0]);
   }

   public ReflectiveClass(Class<?> clazz, Constructor<?>[] constructors, Field[] fields, Method[] methods) {
      this.clazz = clazz;
      this.constructors = constructors;
      this.fields = fields;
      this.methods = methods;
   }

   public void register() {
      RuntimeReflection.register(clazz);
      RuntimeReflection.register(constructors);
      RuntimeReflection.register(fields);
      RuntimeReflection.register(methods);
   }

   public Json toJson() {
      Json j = Json.object();
      j.set("name", clazz.getName());
      if (fields.length > 0) {
         j.set("fields", Json.make(
               Arrays.stream(fields)
                     .map(f -> Json.object().set("name", f.getName()))
                     .collect(Collectors.toList())
         ));
      }

      if (constructors.length > 0 || methods.length > 0) {
         Json methodArray = Json.array();
         for (Constructor<?> c : constructors) {
            methodArray.add(
                  Json.object()
                        .set("name", "<init>")
                        .set("parameterTypes", Json.make(
                              Arrays.stream(c.getParameterTypes())
                                    .map(Class::getName)
                                    .collect(Collectors.toList())
                        ))
            );
         }

         for (Method m : methods) {
            methodArray.add(
                  Json.object()
                        .set("name", m.getName())
                        .set("parameterTypes", Json.make(
                              Arrays.stream(m.getParameterTypes())
                                    .map(Class::getName)
                                    .collect(Collectors.toList())
                        ))
            );
         }
         j.set("methods", methodArray);
      }
      return j;
   }
}
