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

   public Model(Module module, Map<String, AnnotatedType> annotatedTypes, Map<String, ParsedType> parsedTypes,
                Map<String, Package> packages) {
      this.module = module;
      this.annotatedTypes = annotatedTypes;
      this.parsedTypes = parsedTypes;
      this.packages = packages;
   }

   public static class Module {
      final InfinispanModule moduleAnnotation;
      final String moduleClassName;
      final String packageName;
      final String classPrefix;

      public Module(InfinispanModule moduleAnnotation, String moduleClassName, String packageName,
                    String classPrefix) {
         this.moduleAnnotation = moduleAnnotation;
         this.moduleClassName = moduleClassName;
         this.packageName = packageName;
         this.classPrefix = classPrefix;
      }
   }

   public static class ParsedType {
      final TypeElement typeElement;
      final String qualifiedName;
      final String packageName;
      final List<String> code;

      public ParsedType(TypeElement typeElement, String qualifiedName, String packageName,
                        List<String> code) {
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

      public AnnotatedType(TypeElement typeElement, String qualifiedName, String binaryName,
                           String packageName) {
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

   public static class LifecycleMethod {
      final String name;
      final int priority;

      public LifecycleMethod(String name, int priority) {
         this.name = name;
         this.priority = priority;
      }
   }

   public static class InjectMethod {
      final String name;
      final List<InjectField> parameters;

      public InjectMethod(String name,
                          List<InjectField> parameters) {
         this.name = name;
         this.parameters = parameters;
      }
   }

   public static class InjectField {
      final String name;
      final String typeName;
      final String componentName;
      final boolean isComponentRef;

      public InjectField(String name, String typeName, String componentName, boolean isComponentRef) {
         this.name = name;
         this.typeName = typeName;
         this.componentName = componentName;
         this.isComponentRef = isComponentRef;
      }
   }

   public static class MComponent {
      final MBean mbean;
      final String superBinaryName;

      List<MAttribute> attributes = new ArrayList<>();
      List<MOperation> operations = new ArrayList<>();

      public MComponent(MBean mbean, String superBinaryName) {
         this.mbean = mbean;
         this.superBinaryName = superBinaryName;
      }
   }

   public static class MAttribute {
      final String name;
      final ManagedAttribute attribute;
      final boolean useSetter;
      final String type;
      final boolean is;

      public MAttribute(String name, ManagedAttribute attribute, boolean useSetter, String type, boolean is) {
         this.name = name;
         this.attribute = attribute;
         this.useSetter = useSetter;
         this.type = type;
         this.is = is;
      }
   }

   public static class MOperation {
      final String name;
      final ManagedOperation operation;
      final String returnType;
      final List<MParameter> parameters;

      public MOperation(String name, ManagedOperation operation, String returnType, List<MParameter> parameters) {
         this.name = name;
         this.operation = operation;
         this.returnType = returnType;
         this.parameters = parameters;
      }
   }

   public static class MParameter {
      final String name;
      final String type;
      final String description;

      public MParameter(String name, String type, String description) {
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
