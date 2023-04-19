package org.infinispan.commons.graalvm;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.graalvm.nativeimage.hosted.Feature;
import org.jboss.jandex.AnnotationInstance;
import org.jboss.jandex.AnnotationTarget;
import org.jboss.jandex.ClassInfo;
import org.jboss.jandex.DotName;
import org.jboss.jandex.IndexView;

public class ReflectionProcessor {

   final Feature.FeatureAccess featureAccess;
   final IndexView index;

   final List<ReflectiveClass> reflectiveClasses;

   public ReflectionProcessor(Feature.FeatureAccess featureAccess, IndexView index) {
      this.featureAccess = featureAccess;
      this.index = index;
      this.reflectiveClasses = new ArrayList<>();
   }

   public ReflectionProcessor addClasses(Class<?>... classes) {
      return addClasses(false, false, classes);
   }

   public ReflectionProcessor addClasses(boolean fields, boolean methods, Class<?>... classes) {
      reflectiveClasses.addAll(
            Arrays.stream(classes)
                  .map(c -> ReflectiveClass.of(c, fields, methods))
                  .collect(Collectors.toList())
      );
      return this;
   }

   public ReflectionProcessor addClasses(String... classes) {
      return addClasses(false, false, classes);
   }

   public ReflectionProcessor addClasses(boolean fields, boolean methods, String... classes) {
      reflectiveClasses.addAll(
            Arrays.stream(classes)
                  .map(featureAccess::findClassByName)
                  .map(c -> ReflectiveClass.of(c, fields, methods))
                  .collect(Collectors.toList())
      );
      return this;
   }

   public ReflectionProcessor addClassesWithAnnotation(boolean fields, boolean methods, Class<?> annotation) {
      return forEachAnnotation(annotation, instance -> {
         AnnotationTarget target = instance.target();
         if (target.kind() == AnnotationTarget.Kind.CLASS) {
            DotName targetName = target.asClass().name();
            addClasses(fields, methods, targetName.toString());
         }
      });
   }

   public ReflectionProcessor addClassFromAnnotationValue(boolean fields, boolean methods, Class<?> annotation) {
      return forEachAnnotation(annotation, instance -> {
         String className = instance.value().asString();
         addClasses(fields, methods, className);
      });
   }

   public ReflectionProcessor forEachAnnotation(Class<?> annotation, Consumer<AnnotationInstance> consumer) {
      if (!annotation.isAnnotation())
         throw new IllegalArgumentException("Provided class must be an annotation");

      index.getAnnotations(
            DotName.createSimple(annotation.getName())
      ).forEach(consumer);
      return this;
   }

   public ReflectionProcessor addImplementation(boolean fields, boolean methods, String className) {
      return addImplementations(fields, methods, featureAccess.findClassByName(className));
   }

   public ReflectionProcessor addImplementation(boolean fields, boolean methods, Class<?> clazz) {
      Collection<ClassInfo> classInfos;
      if (clazz.isInterface()) {
         classInfos = index.getAllKnownImplementors(DotName.createSimple(clazz.getName()));
      } else {
         classInfos = index.getAllKnownSubclasses(DotName.createSimple(clazz.getName()));
      }

      classInfos.stream()
            .map(ClassInfo::toString)
            .forEach(c -> addClasses(fields, methods, c));
      return this;
   }

   public ReflectionProcessor addImplementations(boolean fields, boolean methods, String... classes) {
      for (String clazz : classes)
         addImplementation(fields, methods, clazz);
      return this;
   }

   public ReflectionProcessor addImplementations(boolean fields, boolean methods, Class<?>... classes) {
      for (Class<?> clazz : classes)
         addImplementation(fields, methods, clazz);
      return this;
   }

   public Stream<ReflectiveClass> classes() {
      return reflectiveClasses.stream();
   }
}
