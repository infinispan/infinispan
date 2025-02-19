package org.infinispan.component.processor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.lang.model.element.TypeElement;

import org.infinispan.factories.annotations.InfinispanModule;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;

/**
 * Information about annotated classes being processed.
 *
 * @author Dan Berindei
 * @since 10.0
 */
public class Model {
   final Module module;
   final Map<String, AnnotatedType> annotatedTypes;
   final Map<String, ParsedType> parsedTypes;
   final Map<String, Package> packages;

   Model(Module module, Map<String, AnnotatedType> annotatedTypes, Map<String, ParsedType> parsedTypes,
         Map<String, Package> packages) {
      this.module = module;
      this.annotatedTypes = annotatedTypes;
      this.parsedTypes = parsedTypes;
      this.packages = packages;
   }

   public static class Module {
      final InfinispanModule moduleAnnotation;
      final TypeElement typeElement;
      final String moduleClassName;
      final String packageName;
      final String classPrefix;

      public Module(InfinispanModule moduleAnnotation, TypeElement typeElement, String packageName, String classPrefix) {
         this.moduleAnnotation = moduleAnnotation;
         this.typeElement = typeElement;
         this.moduleClassName = typeElement.getQualifiedName().toString();
         this.packageName = packageName;
         this.classPrefix = classPrefix;
      }
   }

   record ParsedType(TypeElement typeElement, String qualifiedName, String packageName, List<String> code) {
   }

   public static class AnnotatedType {
      final TypeElement typeElement;
      final String qualifiedName;
      final String binaryName;
      final String packageName;

      Component component;
      MComponent mComponent;

      AnnotatedType(TypeElement typeElement, String qualifiedName, String binaryName, String packageName) {
         this.typeElement = typeElement;
         this.binaryName = binaryName;
         this.qualifiedName = qualifiedName;
         this.packageName = packageName;
      }
   }

   public static class Component {
      final Scope scope;
      final boolean survivesRestarts;
      final List<String> factoryComponentNames;
      final boolean autoInstantiable;
      final String superBinaryName;

      final List<InjectField> injectFields = new ArrayList<>();
      final List<InjectMethod> injectMethods = new ArrayList<>();
      final List<LifecycleMethod> startMethods = new ArrayList<>();
      final List<LifecycleMethod> stopMethods = new ArrayList<>();

      public Component(Scope scope, boolean survivesRestarts, List<String> factoryComponentNames,
                       boolean autoInstantiable, String superBinaryName) {
         this.scope = scope;
         this.survivesRestarts = survivesRestarts;
         this.superBinaryName = superBinaryName;
         this.factoryComponentNames = factoryComponentNames;
         this.autoInstantiable = autoInstantiable;
      }

      boolean hasDependenciesOrLifecycle() {
         return !injectFields.isEmpty() || !injectMethods.isEmpty() || hasLifecycle();
      }

      private boolean hasLifecycle() {
         return !startMethods.isEmpty() || !stopMethods.isEmpty();
      }
   }

   record LifecycleMethod(String name) {
   }

   record InjectMethod(String name, List<InjectField> parameters) {
   }

   record InjectField(String name, String typeName, String componentName, boolean isComponentRef) {
   }

   static class MComponent {
      final MBean mbean;
      final String superBinaryName;

      final List<MAttribute> attributes = new ArrayList<>();
      final List<MOperation> operations = new ArrayList<>();

      MComponent(MBean mbean, String superBinaryName) {
         this.mbean = mbean;
         this.superBinaryName = superBinaryName;
      }
   }

   record MAttribute(String name, String propertyAccessor, ManagedAttribute attribute, boolean useSetter, String type,
                     String boxedType, boolean is) {
   }

   record MOperation(String name, ManagedOperation operation, String returnType, List<MParameter> parameters) {
   }

   record MParameter(String name, String type, String description) {
   }

   static class Package {
      final String packageName;
      final List<TypeElement> typeElements = new ArrayList<>();
      final List<AnnotatedType> annotatedTypes = new ArrayList<>();
      final List<ParsedType> parsedTypes = new ArrayList<>();

      Package(String packageName) {
         this.packageName = packageName;
      }
   }
}
