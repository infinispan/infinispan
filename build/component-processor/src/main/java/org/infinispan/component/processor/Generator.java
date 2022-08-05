package org.infinispan.component.processor;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.NoSuchFileException;
import java.time.Instant;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.processing.Filer;
import javax.lang.model.element.TypeElement;
import javax.tools.FileObject;
import javax.tools.JavaFileObject;
import javax.tools.StandardLocation;

import org.infinispan.factories.annotations.InfinispanModule;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.jmx.annotations.DataType;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;

/**
 * Generate component accessors.
 *
 * @author Dan Berindei
 * @since 10.0
 */
public class Generator {

   private static final String MODULE_METADATA_BUILDER_SERVICE_NAME =
      "META-INF/services/org.infinispan.factories.impl.ModuleMetadataBuilder";

   private final Model model;
   private final Filer filer;

   public Generator(Model model, Filer filer) {
      this.model = model;
      this.filer = filer;
   }

   public void writePackageClasses() throws IOException {
      for (Model.Package p : model.packages.values()) {
         writePackageClass(p);
      }
   }

   private void writePackageClass(Model.Package p) throws IOException {
      String packageClassName = String.format("%s.%sPackageImpl", p.packageName, model.module.classPrefix);
      // IntelliJ will delete the generated file before compilation if any of the source elements has changed
      // We need the old PackageImpl source to parse the accessors of any classes not compiled in this round,
      // so we don't set any source elements.
      JavaFileObject packageFile = filer.createSourceFile(packageClassName);
      try (PrintWriter writer = new PrintWriter(packageFile.openWriter(), false)) {
         writer.printf("package %s;%n%n", p.packageName);
         writer.printf("import java.util.Arrays;%n");
         writer.printf("import java.util.Collections;%n");
         writer.printf("import javax.annotation.processing.Generated;%n");
         writer.printf("%n");
         writer.printf("import org.infinispan.factories.impl.ComponentAccessor;%n");
         writer.printf("import org.infinispan.factories.impl.ModuleMetadataBuilder;%n");
         writer.printf("import org.infinispan.factories.impl.MBeanMetadata;%n");
         writer.printf("import org.infinispan.factories.impl.MBeanMetadata.AttributeMetadata;%n");
         writer.printf("import org.infinispan.factories.impl.MBeanMetadata.OperationMetadata;%n");
         writer.printf("import org.infinispan.factories.impl.MBeanMetadata.OperationParameterMetadata;%n");
         writer.printf("import org.infinispan.factories.impl.WireContext;%n");
         writer.printf("import org.infinispan.lifecycle.ModuleLifecycle;%n");
         writer.printf("%n");
         writer.printf("/**%n * @api.private %n */%n");
         writer.printf("@Generated(value = \"%s\", date = \"%s\")%n", getClass().getName(), Instant.now().toString());
         writer.printf("public final class %sPackageImpl {%n", model.module.classPrefix);
         writer.printf("   public static void registerMetadata(ModuleMetadataBuilder.ModuleBuilder builder) {%n");

         for (Model.AnnotatedType c : p.annotatedTypes) {
            writer.printf("//start %s%n", c.typeElement.getQualifiedName());

            if (c.component != null) {
               writeComponentAccessor(writer, c);
               writer.printf("%n");
            }

            if (c.mComponent != null) {
               writeMBeanMetadata(writer, c);
               writer.printf("%n");
            }
         }

         for (Model.ParsedType parsedType : p.parsedTypes) {
            for (String line : parsedType.code) {
               writer.println(line);
            }
         }

         writer.printf("//end%n");
         writer.printf("   }%n");
         writer.printf("}%n");
      }
   }

   private void writeComponentAccessor(PrintWriter writer, Model.AnnotatedType annotatedType) {
      CharSequence simpleClassName = annotatedType.qualifiedName.substring(annotatedType.packageName.length() + 1);
      CharSequence binaryName = annotatedType.binaryName;

      Model.Component c = annotatedType.component;
      Scopes scope = c.scope.value();
      String scopeLiteral = String.valueOf(scope.ordinal());
      boolean survivesRestarts = c.survivesRestarts;
      boolean autoInstantiable = c.autoInstantiable;
      CharSequence superAccessor = stringLiteral(c.superBinaryName);
      CharSequence eagerDependencies = listLiteral(getEagerDependencies(c));
      CharSequence factoryComponents = listLiteral(c.factoryComponentNames);

      writer.printf("      builder.registerComponentAccessor(\"%s\",%n", binaryName);
      writer.printf("         %s,%n", factoryComponents);

      if (!c.hasDependenciesOrLifecycle() && !autoInstantiable) {
         // Component doesn't need an anonymous class, eagerDependencies is always empty
         writer.printf("         new ComponentAccessor<%s>(\"%s\",%n" +
                       "            %s, %s, %s,%n" +
                       "            %s));%n",
                       simpleClassName, binaryName, scopeLiteral, survivesRestarts,
                       superAccessor, eagerDependencies);
         return;
      }

      writer.printf("         new ComponentAccessor<%s>(\"%s\",%n" +
                    "            %s, %s, %s,%n" +
                    "            %s) {%n",
                    simpleClassName, binaryName, scopeLiteral, survivesRestarts,
                    superAccessor, eagerDependencies);

      if (!c.injectFields.isEmpty() || !c.injectMethods.isEmpty()) {
         writer.printf("         protected void wire(%s instance, WireContext context, boolean start) {%n",
                       simpleClassName);
         for (Model.InjectField injectField : c.injectFields) {
            String componentType = injectField.typeName;
            CharSequence componentName = injectField.componentName;
            String lazy = injectField.isComponentRef ? "Lazy" : "";
            writer.printf("            instance.%s = context.get%s(\"%s\", %s.class, start);%n",
                          injectField.name, lazy, componentName, componentType);
         }
         for (Model.InjectMethod injectMethod : c.injectMethods) {
            writer.printf("            instance.%s(%n", injectMethod.name);
            List<Model.InjectField> parameters = injectMethod.parameters;
            for (int i = 0; i < parameters.size(); i++) {
               Model.InjectField parameter = parameters.get(i);
               String componentType = parameter.typeName;
               CharSequence componentName = parameter.componentName;
               String lazy = parameter.isComponentRef ? "Lazy" : "";
               writer.printf("               context.get%s(\"%s\", %s.class, start)%s%n",
                             lazy, componentName, componentType, optionalComma(i, parameters.size()));
            }
            writer.printf("            );%n");
         }
         writer.printf("         }%n");
         writer.printf("%n");
      }

      if (!c.startMethods.isEmpty() || !c.postStartMethods.isEmpty()) {
         writer.printf("         protected void start(%s instance) throws Exception {%n", simpleClassName);
         writeLifecycleMethodInvocations(writer, c.startMethods);
         writeLifecycleMethodInvocations(writer, c.postStartMethods);
         writer.printf("         }%n");
         writer.printf("%n");
      }

      if (!c.stopMethods.isEmpty()) {
         writer.printf("         protected void stop(%s instance) throws Exception {%n", simpleClassName);
         writeLifecycleMethodInvocations(writer, c.stopMethods);
         writer.printf("         }%n");
         writer.printf("%n");
      }

      if (autoInstantiable) {
         writer.printf("         protected %s newInstance() {%n", simpleClassName);
         writer.printf("            return new %s();%n", simpleClassName);
         writer.printf("         }%n");
         writer.printf("%n");
      }

      writer.printf("      });%n");
   }

   private void writeLifecycleMethodInvocations(PrintWriter writer, List<Model.LifecycleMethod> methods) {
      for (Model.LifecycleMethod method : sortByPriority(methods)) {
         writer.printf("            instance.%s();%n", method.name);
      }
   }

   private List<Model.LifecycleMethod> sortByPriority(List<Model.LifecycleMethod> methods) {
      List<Model.LifecycleMethod> result = new ArrayList<>(methods);
      result.sort(Comparator.comparing(method -> method.priority));
      return result;
   }

   private List<CharSequence> getEagerDependencies(Model.Component c) {
      List<CharSequence> eagerDependencies = new ArrayList<>();
      for (Model.InjectField injectField : c.injectFields) {
         if (!injectField.isComponentRef) {
            eagerDependencies.add(injectField.componentName);
         }
      }
      for (Model.InjectMethod injectMethod : c.injectMethods) {
         for (Model.InjectField parameter : injectMethod.parameters) {
            if (!parameter.isComponentRef) {
               eagerDependencies.add(parameter.componentName);
            }
         }
      }
      return eagerDependencies;
   }

   private void writeMBeanMetadata(PrintWriter writer, Model.AnnotatedType c) {
      CharSequence binaryName = c.binaryName;
      Model.MComponent m = c.mComponent;
      MBean mbean = m.mbean;
      CharSequence superMBeanName = stringLiteral(m.superBinaryName);
      List<Model.MAttribute> attributes = m.attributes;
      List<Model.MOperation> operations = m.operations;

      writer.printf("      builder.registerMBeanMetadata(\"%s\",%n", binaryName);

      int count = attributes.size() + operations.size();
      writer.printf("         MBeanMetadata.of(\"%s\", \"%s\", %s%s%n",
                    mbean.objectName(), mbean.description(), superMBeanName, optionalComma(-1, count));

      int i = 0;
      for (Model.MAttribute attribute : attributes) {
         writeManagedAttribute(writer, attribute.name, attribute.attribute, attribute.useSetter, attribute.type,
               attribute.is, makeGetterFunction(c, attribute), makeSetterFunction(c, attribute), optionalComma(i++, count));
      }
      for (Model.MOperation method : operations) {
         ManagedOperation operation = method.operation;
         // OperationMetadata(String methodName, String operationName, String description, String returnType,
         //    OperationParameterMetadata... methodParameters)
         List<Model.MParameter> parameters = method.parameters;
         writer.printf("            new OperationMetadata(\"%s\", \"%s\", \"%s\", \"%s\"%s%n",
                       method.name, operation.name(), operation.description(),
                       method.returnType, optionalComma(-1, parameters.size()));
         for (int j = 0; j < parameters.size(); j++) {
            Model.MParameter parameter = parameters.get(j);
            // OperationParameterMetadata(String name, String type, String description)
            writer.printf("               new OperationParameterMetadata(\"%s\", \"%s\", \"%s\")%s%n",
                          parameter.name, parameter.type, parameter.description,
                          optionalComma(j, parameters.size()));
         }
         writer.printf("            )%s%n", optionalComma(i++, count));
      }
      writer.printf("      ));%n");
   }

   private String makeGetterFunction(Model.AnnotatedType clazz, Model.MAttribute attribute) {
      // provide accessor function only for a select list of types that are interesting for metrics
      if (attribute.attribute.dataType() == DataType.MEASUREMENT && (
            attribute.boxedType.equals("java.lang.Integer") || attribute.boxedType.equals("java.lang.Long") ||
                  attribute.boxedType.equals("java.lang.Short") || attribute.boxedType.equals("java.lang.Byte") ||
                  attribute.boxedType.equals("java.lang.Float") || attribute.boxedType.equals("java.lang.Double") ||
                  attribute.boxedType.equals("java.math.BigDecimal") || attribute.boxedType.equals("java.math.BigInteger"))) {
         return "(java.util.function.Function<" + clazz.qualifiedName + ", ?>) "
               + (attribute.useSetter ? clazz.qualifiedName + "::" : "_x -> _x.") + attribute.propertyAccessor;
      }
      return "null";
   }

   private String makeSetterFunction(Model.AnnotatedType clazz, Model.MAttribute attribute) {
      // no need for setter unless it is a histogram or timer
      if (attribute.attribute.dataType() == DataType.HISTOGRAM || attribute.attribute.dataType() == DataType.TIMER) {
         return "(" + clazz.qualifiedName + " _x, Object _y) -> _x." + attribute.propertyAccessor
               + (attribute.useSetter ? "((" + attribute.boxedType + ") _y)" : " = (" + attribute.boxedType + ") _y");
      }
      return "null";
   }

   private void writeManagedAttribute(PrintWriter writer, CharSequence name, ManagedAttribute attribute,
                                      boolean useSetter, String type, boolean is, String getterFunction, String setterFunction, String comma) {
      // AttributeMetadata(String name, String description, boolean writable, boolean useSetter, String type, boolean is, Function<?, ?> getterFunction, BiConsumer<?, ?> setterFunction)
      writer.printf("            new AttributeMetadata(\"%s\", \"%s\", %b, %b, \"%s\", %s, %s, %s)%s%n",
            name, attribute.description(), attribute.writable(), useSetter, type, is, getterFunction, setterFunction, comma);
   }

   public void writeModuleClass(TypeElement[] sourceElements) throws IOException {
      InfinispanModule moduleAnnotation = model.module.moduleAnnotation;
      Model.Module module = model.module;
      String moduleClassName = String.format("%s.%sModuleImpl", module.packageName, module.classPrefix);
      JavaFileObject moduleFile = filer.createSourceFile(moduleClassName, sourceElements);
      try (PrintWriter writer = new PrintWriter(moduleFile.openWriter(), false)) {
         writer.printf("package %s;%n%n", module.packageName);
         writer.printf("import java.util.Arrays;%n");
         writer.printf("import java.util.Collection;%n");
         writer.printf("import java.util.Collections;%n");
         writer.printf("import javax.annotation.processing.Generated;%n");
         writer.printf("%n");
         writer.printf("import org.infinispan.factories.impl.ModuleMetadataBuilder;%n");
         writer.printf("import org.infinispan.lifecycle.ModuleLifecycle;%n");
         writer.printf("import org.infinispan.manager.ModuleRepository;%n");
         writer.printf("%n");
         writer.printf("/**%n * @api.private %n */%n");
         writer.printf("@Generated(value = \"%s\", date = \"%s\")%n", getClass().getName(), Instant.now().toString());
         writer.printf("public final class %sModuleImpl implements ModuleMetadataBuilder {%n", module.classPrefix);

         writer.printf("//module %s%n", module.moduleClassName);
         writer.printf("   public String getModuleName() {%n");
         writer.printf("      return \"%s\";%n", moduleAnnotation.name());
         writer.printf("   }%n");
         writer.printf("%n");

         writer.printf("   public Collection<String> getRequiredDependencies() {%n");
         writer.printf("      return %s;%n", listLiteral(Arrays.asList(moduleAnnotation.requiredModules())));
         writer.printf("   }%n");
         writer.printf("%n");

         writer.printf("   public Collection<String> getOptionalDependencies() {%n");
         writer.printf("      return %s;%n", listLiteral(Arrays.asList(moduleAnnotation.optionalModules())));
         writer.printf("   }%n");
         writer.printf("%n");

         writer.printf("   public ModuleLifecycle newModuleLifecycle() {%n");
         writer.printf("      return new %s();%n", module.moduleClassName);
         writer.printf("   }%n");
         writer.printf("%n");

         writer.printf("   public void registerMetadata(ModuleBuilder builder) {%n");
         for (String packageName : model.packages.keySet()) {
            writer.printf("//package %s%n", packageName);
            writer.printf("      %s.%sPackageImpl.registerMetadata(builder);%n", packageName, module.classPrefix);
         }

         writer.printf("   }%n");
         writer.printf("}%n");
      }
   }

   public void writeServiceFile(TypeElement[] sourceElements) throws IOException {
      FileObject serviceFile = filer.createResource(StandardLocation.CLASS_OUTPUT, "",
                                                    MODULE_METADATA_BUILDER_SERVICE_NAME, sourceElements);
      try (PrintWriter writer = new PrintWriter(serviceFile.openWriter(), false)) {
         writer.printf("%s.%sModuleImpl", model.module.packageName, model.module.classPrefix);
      }
   }

   public static String readServiceFile(Filer filer) throws IOException {
      try {
         FileObject serviceFile = filer.getResource(StandardLocation.CLASS_OUTPUT, "",
                                                    MODULE_METADATA_BUILDER_SERVICE_NAME);
         try (BufferedReader reader = new BufferedReader(serviceFile.openReader(false))) {
            return reader.readLine();
         }
      } catch (FileNotFoundException | NoSuchFileException e) {
         return null;
      }
   }

   public static Map.Entry<String, Set<String>> readModuleClass(Filer filer, String moduleImplementationName)
      throws IOException {
      String moduleImplPath = String.format("%s.java", moduleImplementationName.replace(".", "/"));
      try {
         FileObject sourceFile = filer.getResource(StandardLocation.SOURCE_OUTPUT, "", moduleImplPath);
         try (BufferedReader reader = new BufferedReader(sourceFile.openReader(false))) {
            String moduleClass = null;
            Set<String> packages = new HashSet<>();
            String line;
            while ((line = reader.readLine()) != null) {
               Matcher matcher = Pattern.compile("//(module|package) (.*)").matcher(line);
               if (matcher.matches()) {
                  if (matcher.group(1).equals("module")) {
                     moduleClass = matcher.group(2);
                  } else { // package
                     packages.add(matcher.group(2));
                  }
               }
            }
            return new AbstractMap.SimpleEntry<>(moduleClass, packages);
         }
      } catch (FileNotFoundException | NoSuchFileException e) {
         // Module class does not exist, ignore it
         return null;
      }
   }

   public static Map<String, List<String>> readPackageClass(Filer filer, String packageName, CharSequence classPrefix)
      throws IOException {
      String packageImplPath = String.format("%s/%sPackageImpl.java", packageName.replace(".", "/"), classPrefix);
      try {
         FileObject sourceFile = filer.getResource(StandardLocation.SOURCE_OUTPUT, "", packageImplPath);
         try (BufferedReader reader = new BufferedReader(sourceFile.openReader(false))) {
            Map<String, List<String>> components = new HashMap<>();
            String currentComponent = null;
            List<String> currentSource = null;
            String line;
            while ((line = reader.readLine()) != null) {
               Matcher matcher = Pattern.compile("//(start |end)(.*)").matcher(line);
               if (matcher.matches()) {
                  if (matcher.group(1).equals("start ")) {
                     if (currentComponent != null) {
                        components.put(currentComponent, currentSource);
                     }
                     currentComponent = matcher.group(2);
                     currentSource = new ArrayList<>();
                     currentSource.add(line);
                  } else { // end
                     components.put(currentComponent, currentSource);
                     currentComponent = null;
                     currentSource = null;
                  }
               } else if (currentComponent != null) {
                  currentSource.add(line);
               }
            }
            return components;
         }
      } catch (FileNotFoundException | NoSuchFileException e) {
         // Module class does not exist, ignore it
         return null;
      }
   }

   private CharSequence stringLiteral(String value) {
      return value == null ? null : "\"" + value + '"';
   }

   private CharSequence listLiteral(List<? extends CharSequence> list) {
      if (list == null || list.isEmpty())
         return "Collections.emptyList()";

      StringBuilder sb = new StringBuilder("Arrays.asList(");
      boolean first = true;
      for (CharSequence componentName : list) {
         if (first) {
            first = false;
         } else {
            sb.append(", ");
         }
         sb.append('"').append(componentName).append('"');
      }
      sb.append(')');
      return sb;
   }

   private String optionalComma(int index, int size) {
      return index + 1 < size ? "," : "";
   }
}
