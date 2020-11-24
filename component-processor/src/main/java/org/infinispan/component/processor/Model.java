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

   static class ParsedType {
      final TypeElement typeElement;
      final String qualifiedName;
      final String packageName;
      final List<String> code;

      ParsedType(TypeElement typeElement, String qualifiedName, String packageName, List<String> code) {
         this.typeElement = typeElement;
         this.qualifiedName = qualifiedName;
         this.packageName = packageName;
         this.code = code;
      }
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
      final List<LifecycleMethod> postStartMethods = new ArrayList<>();
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
         return !startMethods.isEmpty() || !postStartMethods.isEmpty() || !stopMethods.isEmpty();
      }
   }

   static class LifecycleMethod {
      final String name;
      final int priority;

      LifecycleMethod(String name, int priority) {
         this.name = name;
         this.priority = priority;
      }
   }

   static class InjectMethod {
      final String name;
      final List<InjectField> parameters;

      InjectMethod(String name, List<InjectField> parameters) {
         this.name = name;
         this.parameters = parameters;
      }
   }

   static class InjectField {
      final String name;
      final String typeName;
      final String componentName;
      final boolean isComponentRef;

      InjectField(String name, String typeName, String componentName, boolean isComponentRef) {
         this.name = name;
         this.typeName = typeName;
         this.componentName = componentName;
         this.isComponentRef = isComponentRef;
      }
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

   static class MAttribute {
      final String name;
      final String propertyAccessor;
      final ManagedAttribute attribute;
      final boolean useSetter;
      final String type;
      final String boxedType;
      final boolean is;

      MAttribute(String name, String propertyAccessor, ManagedAttribute attribute, boolean useSetter, String type, String boxedType, boolean is) {
         this.name = name;
         this.propertyAccessor = propertyAccessor;
         this.attribute = attribute;
         this.useSetter = useSetter;
         this.type = type;
         this.boxedType = boxedType;
         this.is = is;
      }
   }

   static class MOperation {
      final String name;
      final ManagedOperation operation;
      final String returnType;
      final List<MParameter> parameters;

      MOperation(String name, ManagedOperation operation, String returnType, List<MParameter> parameters) {
         this.name = name;
         this.operation = operation;
         this.returnType = returnType;
         this.parameters = parameters;
      }
   }

   static class MParameter {
      final String name;
      final String type;
      final String description;

      MParameter(String name, String type, String description) {
         this.name = name;
         this.type = type;
         this.description = description;
      }
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
