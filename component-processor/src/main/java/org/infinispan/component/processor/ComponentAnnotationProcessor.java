package org.infinispan.component.processor;

import static org.infinispan.component.processor.AnnotationTypeValuesExtractor.getTypeValues;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.annotation.Annotation;
import java.nio.file.NoSuchFileException;
import java.time.Instant;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.Filer;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.Processor;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.annotation.processing.SupportedSourceVersion;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.Name;
import javax.lang.model.element.PackageElement;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Elements;
import javax.lang.model.util.Types;
import javax.tools.Diagnostic;
import javax.tools.FileObject;
import javax.tools.JavaFileObject;
import javax.tools.StandardLocation;

import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.factories.annotations.InfinispanModule;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.PostStart;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.annotations.SurvivesRestarts;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.jmx.annotations.Parameter;
import org.kohsuke.MetaInfServices;

@SupportedSourceVersion(SourceVersion.RELEASE_8)
@SupportedAnnotationTypes({ComponentAnnotationProcessor.INFINISPAN_MODULE,
                           ComponentAnnotationProcessor.DEFAULT_FACTORY_FOR,
                           ComponentAnnotationProcessor.SURVIVES_RESTARTS,
                           ComponentAnnotationProcessor.SCOPE,
                           ComponentAnnotationProcessor.MBEAN,
                           ComponentAnnotationProcessor.INJECT,
                           ComponentAnnotationProcessor.POST_START,
                           ComponentAnnotationProcessor.START,
                           ComponentAnnotationProcessor.STOP,
                           ComponentAnnotationProcessor.MANAGED_ATTRIBUTE,
                           ComponentAnnotationProcessor.MANAGED_OPERATION,
                           ComponentAnnotationProcessor.MANAGED_OPERATION_PARAMETER})
@MetaInfServices(Processor.class)
public class ComponentAnnotationProcessor extends AbstractProcessor {
   public static final String MODULE_METADATA_BUILDER_SERVICE_NAME =
      "META-INF/services/org.infinispan.modules.ModuleMetadataBuilder";
   static final String INFINISPAN_MODULE = "org.infinispan.factories.annotations.InfinispanModule";
   static final String DEFAULT_FACTORY_FOR = "org.infinispan.factories.annotations.DefaultFactoryFor";
   static final String SURVIVES_RESTARTS = "org.infinispan.factories.annotations.SurvivesRestarts";
   static final String SCOPE = "org.infinispan.factories.scopes.Scope";
   static final String MBEAN = "org.infinispan.jmx.annotations.MBean";

   static final String INJECT = "org.infinispan.factories.annotations.Inject";
   static final String POST_START = "org.infinispan.factories.annotations.PostStart";
   static final String START = "org.infinispan.factories.annotations.Start";
   static final String STOP = "org.infinispan.factories.annotations.Stop";
   static final String MANAGED_ATTRIBUTE = "org.infinispan.jmx.annotations.ManagedAttribute";
   static final String MANAGED_OPERATION = "org.infinispan.jmx.annotations.ManagedOperation";
   static final String MANAGED_OPERATION_PARAMETER = "org.infinispan.jmx.annotations.Parameter";

   static final String FACTORY_CLASSES = "classes";
   static final String COMPONENT_REF_CLASS = "org.infinispan.factories.impl.ComponentRef";
   static final String AUTO_INSTANTIABLE_FACTORY_CLASS = "org.infinispan.factories.AutoInstantiableFactory";
   static final Pattern MODULE_NAME_PATTERN = Pattern.compile("[a-zA-Z][-a-zA-Z0-9]*");
   static final Pattern MODULE_NAME_SEPARATOR_PATTERN = Pattern.compile("-");

   private ModuleData module;
   private boolean errorReported = false;

   @Override
   public synchronized void init(ProcessingEnvironment processingEnv) {
      super.init(processingEnv);

      module = new ModuleData();
   }

   @Override
   public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
      try {
         readGeneratedFiles();

         // Must process root elements first to prune module.generatedSources
         processRootElements(roundEnv);

         processModuleAnnotation(roundEnv);

         processClassAnnotation(roundEnv, Scope.class);

         validateClassAnnotation(roundEnv, DefaultFactoryFor.class, Scope.class);
         validateClassAnnotation(roundEnv, SurvivesRestarts.class, Scope.class);

         processFieldOrMethodAnnotation(roundEnv, module, Inject.class, Scope.class,
                                        ComponentData::addInjectField, ComponentData::addInjectMethod);
         processMethodAnnotation(roundEnv, module, Start.class, Scope.class,
                                 ComponentData::addStartMethod);
         processMethodAnnotation(roundEnv, module, PostStart.class, Scope.class,
                                 ComponentData::addPostStartMethod);
         processMethodAnnotation(roundEnv, module, Stop.class, Scope.class,
                                 ComponentData::addStopMethod);

         processClassAnnotation(roundEnv, MBean.class);
         processMethodAnnotation(roundEnv, module, ManagedOperation.class, MBean.class,
                                 ComponentData::addManagedOperation);
         processFieldOrMethodAnnotation(roundEnv, module, ManagedAttribute.class, MBean.class,
                                                   ComponentData::addManagedAttributeField,
                                                   ComponentData::addManagedAttributeMethod);

         // In theory we could generate the package classes early (or better generate a class for each component)
         // but the classes we generate don't have any annotations for other processors to parse.
         if (roundEnv.processingOver()) {
            if (module.packageName == null) {
               List<Name> componentNames = module.components()
                                                 .map(c -> c.typeElement.getQualifiedName())
                                                 .collect(Collectors.toList());
               error(null, "@InfinispanModule annotation not found in any class, please perform a clean build. " +
                           "Found components %s", componentNames);
               return true;
            }

            for (PackageData packageData : module.packages.values()) {
               validatePackage(packageData);
            }

            // Only generate classes if valid
            if (errorReported || roundEnv.errorRaised())
               return true;

            for (PackageData packageData : module.packages.values()) {
               writePackageClass(packageData);
            }

            TypeElement[] sourceElements = module.components().map(c -> c.typeElement).toArray(TypeElement[]::new);
            writeModuleClass(sourceElements);
            writeServiceFile(sourceElements);
         }
      } catch (Throwable t) {
         String stackTrace = Stream.of(t.getStackTrace())
                                   .map(Object::toString)
                                   .collect(Collectors.joining("\n\tat ", "\tat ", ""));
         error(null, "ComponentAnnotationProcessor unexpected error: %s\n%s", t, stackTrace);
      }
      return true;
   }

   private void processModuleAnnotation(RoundEnvironment roundEnv) {
      Set<? extends Element> moduleClasses = roundEnv.getElementsAnnotatedWith(InfinispanModule.class);
      for (Element element : moduleClasses) {
         TypeElement typeElement = requireType(element, InfinispanModule.class);
         if (typeElement == null)
            return;

         String moduleName = typeElement.getAnnotation(InfinispanModule.class).name();
         if (!MODULE_NAME_PATTERN.matcher(moduleName).matches()) {
            error(element, "@InfinispanModule name attribute must include only letters, digits or `-`");
            return;
         }

         Name moduleClassName = typeElement.getQualifiedName();
         if (module.moduleClassName != null && !moduleClassName.contentEquals(module.moduleClassName)) {
            error(typeElement, "@InfinispanModule allowed on a single class, already present on %s",
                  module.moduleClassName);
            return;
         }

         if (module.moduleClassName == null) {
            Name packageName = elements().getPackageOf(typeElement).getQualifiedName();
            module.setModuleClass(typeElement, packageName);
         }
      }
   }

   private void processRootElements(RoundEnvironment roundEnvironment) {
      for (Element element : roundEnvironment.getRootElements()) {
         if (isTypeElement(element)) {
            module.generatedSources.remove(element);
         }
      }
   }

   private boolean isTypeElement(Element element) {
      return element.getKind().isClass() || element.getKind().isInterface();
   }

   private void readGeneratedFiles() throws IOException {
      // Only read the module once
      if (module.generatedSources != null)
         return;

      String moduleImplementationName = readServiceFile();
      if (moduleImplementationName == null) {
         module.generatedSources = Collections.emptyMap();
         return;
      }

      AbstractMap.SimpleEntry<String, Set<String>> moduleClassAndPackages = readModuleClass(moduleImplementationName);
      if (moduleClassAndPackages == null) {
         info(null, "Ignoring removed or renamed module implementation %s", moduleImplementationName);
         module.generatedSources = Collections.emptyMap();
         return;
      }

      TypeElement moduleClassElement = elements().getTypeElement(moduleClassAndPackages.getKey());
      if (moduleClassElement != null) {
         module.setModuleClass(moduleClassElement, elements().getPackageOf(moduleClassElement).getQualifiedName());
      } else {
         info(null, "Ignoring invalid module implementation %s", moduleImplementationName);
      }

      Set<String> packages = moduleClassAndPackages.getValue();
      Map<TypeElement, List<String>> generatedSources = new HashMap<>();
      for (String packageName : packages) {
         Map<String, List<String>> packageSources = readPackageClass(packageName, module.classPrefix);
         if (packageSources == null) {
            info(null, "Ignoring removed or renamed package implementation %s", packageName);
            continue;
         }
         packageSources.forEach((sourceClassName, source) -> {
            TypeElement sourceType = elements().getTypeElement(sourceClassName);
            // Ignore types that no longer exist
            if (sourceType != null) {
               generatedSources.put(sourceType, source);
            } else {
               info( null, "Ignoring removed component class %s", sourceClassName);
            }
         });
      }

      module.generatedSources = generatedSources;
   }

   private Map<String, List<String>> readPackageClass(String packageName, CharSequence classPrefix) throws IOException {
      String packageImplPath = String.format("%s/%sPackageImpl.java", packageName.replace(".", "/"), classPrefix);
      try {
         FileObject sourceFile = filer().getResource(StandardLocation.SOURCE_OUTPUT, "", packageImplPath);
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

   private AbstractMap.SimpleEntry<String, Set<String>> readModuleClass(String moduleImplementationName) throws IOException {
      String moduleImplPath = String.format("%s.java", moduleImplementationName.replace(".", "/"));
      try {
         FileObject sourceFile = filer().getResource(StandardLocation.SOURCE_OUTPUT, "", moduleImplPath);
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

   private TypeElement getRootElement(ComponentData componentData) {
      TypeElement sourceElement = componentData.typeElement;
      while (isTypeElement(sourceElement.getEnclosingElement())) {
         sourceElement = ((TypeElement) sourceElement.getEnclosingElement());
      }
      return sourceElement;
   }

   private void writePackageClass(PackageData packageData) throws IOException {
      TypeMirror autoInstantiableType = elements().getTypeElement(AUTO_INSTANTIABLE_FACTORY_CLASS).asType();

      TypeElement[] sourceElements = packageData.components.keySet().toArray(new TypeElement[0]);
      String packageClassName = String.format("%s.%sPackageImpl", packageData.packageElement.getQualifiedName(),
                                              module.classPrefix);
      JavaFileObject packageFile = filer().createSourceFile(packageClassName, sourceElements);
      try (PrintWriter writer = new PrintWriter(packageFile.openWriter(), false)) {
         writer.printf("package %s;\n\n", packageData.packageElement.getQualifiedName());
         writer.printf("import java.util.Arrays;\n");
         writer.printf("import java.util.Collections;\n");
         writer.printf("import javax.annotation.Generated;\n");
         writer.printf("\n");
         writer.printf("import org.infinispan.factories.impl.ComponentAccessor;\n");
         writer.printf("import org.infinispan.factories.components.JmxAttributeMetadata;\n");
         writer.printf("import org.infinispan.factories.components.JmxOperationMetadata;\n");
         writer.printf("import org.infinispan.factories.components.JmxOperationParameter;\n");
         writer.printf("import org.infinispan.factories.impl.MBeanMetadata;\n");
         writer.printf("import org.infinispan.factories.impl.WireContext;\n");
         writer.printf("import org.infinispan.manager.ModuleRepository;\n");
         writer.printf("\n");
         writer.printf("@Generated(value = \"%s\", date = \"%s\")\n", getClass().getName(), Instant.now().toString());
         writer.printf("public class %sPackageImpl {\n", module.classPrefix);
         writer.printf("   public static void registerMetadata(ModuleRepository.Builder builder) {\n");

         for (ComponentData c : packageData.components.values()) {
            writer.printf("//start %s\n", c.typeElement.getQualifiedName());

            if (c.hasAccessor()) {
               writeComponentAccessor(autoInstantiableType, writer, c);
               writer.printf("\n");
            }

            if (c.hasMBeanMetadata()) {
               writeMBeanMetadata(writer, c);
               writer.printf("\n");
            }
         }

         module.generatedSources.forEach((sourceElement, source) -> {
            if (elements().getPackageOf(sourceElement).equals(packageData.packageElement)) {
               for (String line : source) {
                  writer.println(line);
               }
            }
         });

         writer.printf("//end\n");
         writer.printf("   }\n");
         writer.printf("}\n");
      }
   }

   private void writeMBeanMetadata(PrintWriter writer, ComponentData c) {
      CharSequence binaryName = binaryName(c.typeElement);
      MBean mbean = c.typeElement.getAnnotation(MBean.class);
      CharSequence superMBeanName = binaryNameLiteral(getSuperClass(c.typeElement, MBean.class));
      List<VariableElement> attributeFields = c.mbeanAttributeFields;
      List<ExecutableElement> attributeMethods = c.mbeanAttributeMethods;
      List<ExecutableElement> operations = c.mbeanOperations;

      writer.printf("      builder.registerMBeanMetadata(\"%s\",\n", binaryName);

      int count = attributeFields.size() + attributeMethods.size() + operations.size();
      writer.printf("         MBeanMetadata.of(\"%s\", \"%s\", %s%s\n",
                    mbean.objectName(), mbean.description(), superMBeanName, optionalComma(-1, count));

      int i = 0;
      for (VariableElement field : attributeFields) {
         ManagedAttribute attribute = field.getAnnotation(ManagedAttribute.class);
         Name name = field.getSimpleName();
         String type = field.asType().toString();
         writeManagedAttribute(writer, name, attribute, false, type, false, optionalComma(i++, count));
      }
      for (ExecutableElement method : attributeMethods) {
         ManagedAttribute attribute = method.getAnnotation(ManagedAttribute.class);
         String methodName = method.getSimpleName().toString();
         String name = extractFieldName(methodName);
         boolean is = methodName.startsWith("is");
         String type;
         if (methodName.startsWith("set")) {
            type = method.getParameters().get(0).asType().toString();
         } else if (methodName.startsWith("get") || is) {
            type = method.getReturnType().toString();
         } else {
            error(method, "Method annotated with @ManagedAttribute does not start with `set`, `get`, or `is`");
            type = "";
         }
         writeManagedAttribute(writer, name, attribute, true, type, is, optionalComma(i++, count));
      }
      for (ExecutableElement method : operations) {
         ManagedOperation operation = method.getAnnotation(ManagedOperation.class);
         // JmxOperationMetadata(String methodName, String operationName, String description, String returnType,
         //    JmxOperationParameter... methodParameters)
         List<? extends VariableElement> parameters = method.getParameters();
         writer.printf("            new JmxOperationMetadata(\"%s\", \"%s\", \"%s\", \"%s\"%s\n",
                       method.getSimpleName(), operation.name(), operation.description(),
                       method.getReturnType().toString(), optionalComma(-1, parameters.size()));
         for (int j = 0; j < parameters.size(); j++) {
            VariableElement parameter = parameters.get(j);
            // JmxOperationParameter(String name, String type, String description)
            Parameter parameterAnnotation = parameter.getAnnotation(Parameter.class);
            CharSequence name = (parameterAnnotation != null && !parameterAnnotation.name().isEmpty()) ?
                                parameterAnnotation.name() : parameter.getSimpleName();
            String type = (parameterAnnotation != null && !parameterAnnotation.type().isEmpty()) ?
                          parameterAnnotation.type() : parameter.asType().toString();
            String description = parameterAnnotation != null ?
                                 parameterAnnotation.description() : null;
            writer.printf("               new JmxOperationParameter(\"%s\", \"%s\", \"%s\")%s\n",
                          name, type, description,
                          optionalComma(j, parameters.size()));
         }
         writer.printf("            )%s\n", optionalComma(i++, count));
      }
      writer.printf("      ));\n");
   }

   private String optionalComma(int index, int size) {
      return index + 1 < size ? "," : "";
   }

   private void writeManagedAttribute(PrintWriter writer, CharSequence name, ManagedAttribute attribute,
                                      boolean useSetter, String type, boolean is, String comma) {
      // JmxAttributeMetadata(String name, String description, boolean writable, boolean useSetter, String type,
      // boolean is)
      writer.printf("            new JmxAttributeMetadata(\"%s\", \"%s\", %b, %b, \"%s\", %s)%s\n",
                    name, attribute.description(), attribute.writable(), useSetter, type, is, comma);
   }

   private static String extractFieldName(String setterOrGetter) {
      String field = null;
      if (setterOrGetter.startsWith("set") || setterOrGetter.startsWith("get"))
         field = setterOrGetter.substring(3);
      else if (setterOrGetter.startsWith("is"))
         field = setterOrGetter.substring(2);

      if (field != null && field.length() > 1) {
         StringBuilder sb = new StringBuilder();
         sb.append(Character.toLowerCase(field.charAt(0)));
         if (field.length() > 2)
            sb.append(field.substring(1));
         return sb.toString();
      }
      return null;
   }

   private void writeComponentAccessor(TypeMirror autoInstantiableType, PrintWriter writer,
                                       ComponentData c) {
      CharSequence simpleClassName = classNameNoPackage(c.typeElement);
      CharSequence binaryName = binaryName(c.typeElement);

      Scopes scope = getScope(c.typeElement);
      String scopeLiteral = scope != null ? String.valueOf(scope.ordinal()) : "null";
      boolean survivesRestarts = getSurvivesRestarts(c.typeElement);
      boolean autoInstantiable = types().isAssignable(c.typeElement.asType(), autoInstantiableType);
      TypeElement superAccessorClass = getSuperClass(c.typeElement, Scope.class);
      CharSequence superAccessor = binaryNameLiteral(superAccessorClass);
      CharSequence eagerDependencies = listLiteral(getEagerDependencies(c));
      CharSequence factoryComponents = listLiteral(getFactoryComponentNames(c));

      writer.printf("      builder.registerComponentAccessor(\"%s\",\n", binaryName);
      writer.printf("         %s,\n", factoryComponents);

      if (!c.hasDependenciesOrLifecycle() && !autoInstantiable) {
         // Component doesn't need an anonymous class, eagerDependencies is always empty
         writer.printf("         new ComponentAccessor<%s>(\"%s\",\n" +
                       "            %s, %s, %s,\n" +
                       "            %s));\n",
                       simpleClassName, binaryName, scopeLiteral, survivesRestarts,
                       superAccessor, eagerDependencies);
         return;
      }

      writer.printf("         new ComponentAccessor<%s>(\"%s\",\n" +
                    "            %s, %s, %s,\n" +
                    "            %s) {\n",
                    simpleClassName, binaryName, scopeLiteral, survivesRestarts,
                    superAccessor, eagerDependencies);

      if (!c.injectFields.isEmpty() || !c.injectMethods.isEmpty()) {
         writer.printf("         protected void wire(%s instance, WireContext context, boolean start) {\n",
                       simpleClassName);
         for (VariableElement injectField : c.injectFields) {
            TypeMirror componentType = getInjectedType(injectField);
            CharSequence componentName = getComponentName(injectField, componentType);
            String lazy = isComponentRef(injectField) ? "Lazy" : "";
            writer.printf("            instance.%s = context.get%s(\"%s\", %s.class, start);\n",
                          injectField.getSimpleName(), lazy, componentName, componentType);
         }
         for (ExecutableElement injectMethod : c.injectMethods) {
            writer.printf("            instance.%s(\n", injectMethod.getSimpleName());
            List<? extends VariableElement> parameters = injectMethod.getParameters();
            for (int i = 0; i < parameters.size(); i++) {
               VariableElement parameter = parameters.get(i);
               TypeMirror componentType = getInjectedType(parameter);
               CharSequence componentName = getComponentName(parameter, componentType);
               String lazy = isComponentRef(parameter) ? "Lazy" : "";
               writer.printf("               context.get%s(\"%s\", %s.class, start)%s",
                             lazy, componentName, componentType, optionalComma(i, parameters.size()));
            }
            writer.printf(");\n");
         }
         writer.printf("         }\n");
         writer.printf("\n");
      }

      if (!c.startMethods.isEmpty() || !c.postStartMethods.isEmpty()) {
         writer.printf("         protected void start(%s instance) throws Exception {\n", simpleClassName);
         writeLifecycleMethodInvocations(writer, c, c.startMethods, Start.class, Start::priority);
         writeLifecycleMethodInvocations(writer, c, c.postStartMethods, PostStart.class, PostStart::priority);
         writer.printf("         }\n");
         writer.printf("\n");

         for (ExecutableElement startMethod : c.postStartMethods) {
            warn(startMethod, "The @PostStart annotation is deprecated and will be ignored in the future");
         }
      }

      if (!c.stopMethods.isEmpty()) {
         writer.printf("         protected void stop(%s instance) throws Exception {\n", simpleClassName);
         writeLifecycleMethodInvocations(writer, c, c.stopMethods, Stop.class, Stop::priority);
         writer.printf("         }\n");
         writer.printf("\n");
      }

      if (autoInstantiable) {
         writer.printf("         protected %s newInstance() {\n", simpleClassName);
         writer.printf("            return new %s();\n", simpleClassName);
         writer.printf("         }\n");
         writer.printf("\n");
      }

      writer.printf("      });\n");
   }

   private <A extends Annotation>
   void writeLifecycleMethodInvocations(PrintWriter writer, ComponentData c, List<ExecutableElement> startMethods,
                                        Class<A> annotationClass, Function<A, Integer> priorityExtractor) {
      for (ExecutableElement startMethod : sortByPriority(startMethods, annotationClass, priorityExtractor)) {
         if (validateLifecycleMethod(startMethod, c.typeElement, annotationClass, priorityExtractor)) {
            writer.printf("            instance.%s();\n", startMethod.getSimpleName());
         }
      }
   }

   private <A extends Annotation>
   Iterable<? extends ExecutableElement> sortByPriority(List<ExecutableElement> methods, Class<A> annotationClass,
                                                        Function<A, Integer> priorityExtractor) {
      List<ExecutableElement> result = new ArrayList<>(methods);
      result.sort(Comparator.comparing(method -> priorityExtractor.apply(method.getAnnotation(annotationClass))));
      return result;
   }

   private <A extends Annotation>
   boolean validateLifecycleMethod(ExecutableElement method, TypeElement typeElement, Class<A> annotationClass,
                                   Function<A, Integer> priorityExtractor) {
      if (!method.getParameters().isEmpty()) {
         error(method, "Lifecycle methods must have no parameters");
         return false;
      }

      int priority = priorityExtractor.apply(method.getAnnotation(annotationClass));
      if (priority != 10) {
         warn(method, "Priority is ignored");
      }

      TypeElement superAccessorClass = getSuperClass(typeElement, Scope.class);
      while (superAccessorClass != null) {
         for (Element element : superAccessorClass.getEnclosedElements()) {
            if (element.getKind() == ElementKind.METHOD &&
                element.getSimpleName().contentEquals(method.getSimpleName()) &&
                element.getAnnotation(annotationClass) != null) {
               // Method is already included in the super class lifecycle, ignore it
               return false;
            }
         }

         superAccessorClass = getSuperClass(superAccessorClass, Scope.class);
      }
      return true;
   }

   private List<CharSequence> getEagerDependencies(ComponentData c) {
      List<CharSequence> eagerDependencies = new ArrayList<>();
      for (VariableElement injectField : c.injectFields) {
         TypeMirror componentType = getInjectedType(injectField);
         if (!isComponentRef(injectField)) {
            eagerDependencies.add(getComponentName(injectField, componentType));
         }
      }
      for (ExecutableElement injectMethod : c.injectMethods) {
         for (VariableElement parameter : injectMethod.getParameters()) {
            TypeMirror componentType = getInjectedType(parameter);
            if (!isComponentRef(parameter)) {
               eagerDependencies.add(getComponentName(parameter, componentType));
            }
         }
      }
      return eagerDependencies;
   }

   private CharSequence listLiteral(List<CharSequence> list) {
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

   private void validatePackage(PackageData packageData) {
      for (ComponentData component : packageData.components.values()) {
         validateInterfaceMethods(component, Inject.class, Scope.class);
         validateInterfaceMethods(component, Start.class, Scope.class);
         validateInterfaceMethods(component, PostStart.class, Scope.class);
         validateInterfaceMethods(component, Stop.class, Scope.class);

         validateInterfaceMethods(component, ManagedAttribute.class, MBean.class);
         validateInterfaceMethods(component, ManagedOperation.class, MBean.class);
      }
   }

   private void validateInterfaceMethods(ComponentData component, Class<? extends Annotation> methodAnnotation,
                                            Class<? extends Annotation> classAnnotation) {
      Set<String> methodsMissingAnnotation = new HashSet<>();
      for (TypeElement interfaceElement : getInterfaces(component.typeElement, classAnnotation)) {
         for (Element member : interfaceElement.getEnclosedElements()) {
            if (member.getKind() == ElementKind.METHOD && member.getAnnotation(methodAnnotation) != null) {
               methodsMissingAnnotation.add(member.toString());
            }
         }
      }

      TypeElement classElement = component.typeElement;
      while (classElement != null) {
         for (Element member : classElement.getEnclosedElements()) {
            if (member.getKind() == ElementKind.METHOD && member.getAnnotation(methodAnnotation) != null) {
               methodsMissingAnnotation.remove(member.toString());
            }
         }
         classElement = getSuperClass(classElement, classAnnotation);
      }

      for (Element member : component.typeElement.getEnclosedElements()) {
         if (member.getKind() == ElementKind.METHOD && methodsMissingAnnotation.contains(member.toString())) {
            error(member, "@%s annotation on interface method %s must be repeated in implementing classes",
                  methodAnnotation.getSimpleName(), member.toString());
         }
      }
   }

   private List<CharSequence> getFactoryComponentNames(ComponentData c) {
      DefaultFactoryFor factoryAnnotation = c.typeElement.getAnnotation(DefaultFactoryFor.class);
      List<CharSequence> componentNames = new ArrayList<>();
      if (factoryAnnotation != null) {
         TypeElement defaultFactoryForType = elements().getTypeElement(DefaultFactoryFor.class.getName());
         List<TypeMirror> producedTypes = getTypeValues(c.typeElement, defaultFactoryForType, FACTORY_CLASSES);
         for (TypeMirror type : producedTypes) {
            componentNames.add(binaryName(type));
         }
         Collections.addAll(componentNames, factoryAnnotation.names());
      }
      return componentNames;
   }

   private CharSequence binaryName(TypeElement typeElement) {
      return elements().getBinaryName(typeElement);
   }

   private CharSequence classNameNoPackage(TypeElement typeElement) {
      Name qualifiedName = typeElement.getQualifiedName();
      Name packageName = elements().getPackageOf(typeElement).getQualifiedName();
      return qualifiedName.subSequence(packageName.length() + 1, qualifiedName.length());
   }

   private TypeMirror getInjectedType(VariableElement injectFieldOrParameter) {
      DeclaredType declaredType = (DeclaredType) injectFieldOrParameter.asType();
      TypeMirror componentType = types().erasure(declaredType);
      if (componentType.toString().equals(COMPONENT_REF_CLASS)) {
         List<? extends TypeMirror> typeArguments = declaredType.getTypeArguments();
         componentType = types().erasure(typeArguments.get(0));
      }
      return componentType;
   }

   private CharSequence getComponentName(VariableElement parameter, TypeMirror componentType) {
      ComponentName nameAnnotation = parameter.getAnnotation(ComponentName.class);
      return nameAnnotation != null ? nameAnnotation.value() : binaryName(componentType);
   }

   private boolean isComponentRef(VariableElement injectFieldOrParameter) {
      String erasedType = types().erasure(injectFieldOrParameter.asType()).toString();
      return COMPONENT_REF_CLASS.equals(erasedType);
   }

   private void writeModuleClass(TypeElement[] sourceElements) throws IOException {
      InfinispanModule moduleAnnotation = module.moduleAnnotation;

      String moduleClassName = String.format("%s.%sModuleImpl", module.packageName, module.classPrefix);
      JavaFileObject moduleFile = filer().createSourceFile(moduleClassName, sourceElements);
      try (PrintWriter writer = new PrintWriter(moduleFile.openWriter(), false)) {
         writer.printf("package %s;\n\n", module.packageName);
         writer.printf("import java.util.Arrays;\n");
         writer.printf("import java.util.Collection;\n");
         writer.printf("import java.util.Collections;\n");
         writer.printf("import java.util.Collections;\n");
         writer.printf("import javax.annotation.Generated;\n");
         writer.printf("\n");
         writer.printf("import org.infinispan.lifecycle.ModuleLifecycle;\n");
         writer.printf("import org.infinispan.manager.ModuleRepository;\n");
         writer.printf("import org.infinispan.modules.ModuleMetadataBuilder;\n");
         writer.printf("\n");
         writer.printf("@Generated(value = \"%s\", date = \"%s\")\n", getClass().getName(), Instant.now().toString());
         writer.printf("public class %sModuleImpl implements ModuleMetadataBuilder {\n", module.classPrefix);

         writer.printf("//module %s\n", module.moduleClassName);
         writer.printf("   public String getModuleName() {\n");
         writer.printf("      return \"%s\";\n", moduleAnnotation.name());
         writer.printf("   };\n");
         writer.printf("\n");

         writer.printf("   public Collection<String> getRequiredDependencies() {\n");
         writer.printf("      return %s;\n", listLiteral(Arrays.asList(moduleAnnotation.requiredModules())));
         writer.printf("   };\n");
         writer.printf("\n");

         writer.printf("   public Collection<String> getOptionalDependencies() {\n");
         writer.printf("      return %s;\n", listLiteral(Arrays.asList(moduleAnnotation.optionalModules())));
         writer.printf("   };\n");
         writer.printf("\n");

         writer.printf("   public ModuleLifecycle newModuleLifecycle() {\n");
         writer.printf("      return new %s();\n", module.moduleClassName);
         writer.printf("   };\n");
         writer.printf("\n");

         writer.printf("   public void registerMetadata(ModuleRepository.Builder builder) {\n");
         for (String packageName : mergePackages()) {
            writer.printf("//package %s\n", packageName);
            writer.printf("      %s.%sPackageImpl.registerMetadata(builder);\n", packageName, module.classPrefix);
         }

         writer.printf("   }\n");
         writer.printf("}\n");
      }
   }

   private void writeServiceFile(TypeElement[] sourceElements) throws IOException {
      FileObject serviceFile = filer().createResource(StandardLocation.CLASS_OUTPUT, "",
                                                      MODULE_METADATA_BUILDER_SERVICE_NAME, sourceElements);
      try (PrintWriter writer = new PrintWriter(serviceFile.openWriter(), false)) {
         writer.printf("%s.%sModuleImpl", module.packageName, module.classPrefix);
      }
   }

   private String readServiceFile() throws IOException {
      try {
         FileObject serviceFile = filer().getResource(StandardLocation.CLASS_OUTPUT, "",
                                                      MODULE_METADATA_BUILDER_SERVICE_NAME);
         try (BufferedReader reader = new BufferedReader(serviceFile.openReader(false))) {
            return reader.readLine();
         }
      } catch (FileNotFoundException | NoSuchFileException e) {
         return null;
      }
   }

   private Set<String> mergePackages() {
      Set<String> packages = new TreeSet<>();
      for (PackageElement p : module.packages.keySet()) {
         packages.add(p.getQualifiedName().toString());
      }
      for (TypeElement sourceElement : module.generatedSources.keySet()) {
         packages.add(elements().getPackageOf(sourceElement).getQualifiedName().toString());
      }
      return packages;
   }

   private CharSequence binaryName(TypeMirror type) {
      return binaryName((TypeElement) types().asElement(type));
   }

   private void processFieldOrMethodAnnotation(RoundEnvironment roundEnv,
                                                  ModuleData module, Class<? extends Annotation> annotationClass,
                                                  Class<? extends Annotation> classAnnotationClass,
                                                  BiConsumer<ComponentData, VariableElement> fieldConsumer,
                                                  BiConsumer<ComponentData, ExecutableElement> methodConsumer) {
      for (Element e : roundEnv.getElementsAnnotatedWith(annotationClass)) {
         if (e.getEnclosingElement().getKind().isClass() || e.getEnclosingElement().getKind().isInterface()) {
            TypeElement classElement = (TypeElement) e.getEnclosingElement();
            ComponentData componentData = module.getComponent(classElement, elements().getPackageOf(e));
            if (componentData != null && classElement.getAnnotation(classAnnotationClass) != null) {
               if (e.getKind().isField()) {
                  fieldConsumer.accept(componentData, (VariableElement) e);
               } else if (e.getKind() == ElementKind.METHOD) {
                  methodConsumer.accept(componentData, (ExecutableElement) e);
               } else {
                  error(e, "%s annotation can only be applied to fields or methods", annotationClass.getSimpleName());
               }
            } else {
               error(e, "Annotation @%s requires annotation @%s on the class",
                     annotationClass.getSimpleName(), classAnnotationClass.getSimpleName());
            }
         } else {
            error(e, "%s annotation can only be applied to fields or methods of classes or interfaces",
                  annotationClass.getSimpleName());
         }
      }
   }

   private <A extends Annotation> void processClassAnnotation(RoundEnvironment roundEnv, Class<A> annotationClass) {
      for (Element e : roundEnv.getElementsAnnotatedWith(annotationClass)) {
         TypeElement classElement = requireType(e, annotationClass);
         if (classElement == null) {
            continue;
         }
         PackageElement packageElement = elements().getPackageOf(e);
         module.addComponent(classElement, packageElement);
      }
   }

   private <A extends Annotation> void validateClassAnnotation(RoundEnvironment roundEnv, Class<A> annotationClass,
                                                                  Class<? extends Annotation> classAnnotationClass) {
      for (Element e : roundEnv.getElementsAnnotatedWith(annotationClass)) {
         TypeElement classElement = requireType(e, annotationClass);
         if (classElement != null) {
            PackageElement packageElement = elements().getPackageOf(e);
            if (module.getComponent(classElement, packageElement) == null) {
               error(e, "Annotation @%s requires annotation @%s on the class (possibly inherited)",
                     annotationClass.getSimpleName(), classAnnotationClass.getSimpleName());
            }
         }
      }
   }

   private void processMethodAnnotation(RoundEnvironment roundEnv, ModuleData module,
                                           Class<? extends Annotation> annotationClass,
                                           Class<? extends Annotation> classAnnotationClass,
                                           BiConsumer<ComponentData, ExecutableElement> annotationConsumer) {
      for (Element e : roundEnv.getElementsAnnotatedWith(annotationClass)) {
         if (e.getEnclosingElement().getKind().isClass() || e.getEnclosingElement().getKind().isInterface()) {
            TypeElement classElement = (TypeElement) e.getEnclosingElement();
            ComponentData componentData = module.getComponent(classElement, elements().getPackageOf(e));
            if (componentData != null) {
               if (e.getKind() == ElementKind.METHOD) {
                  annotationConsumer.accept(componentData, (ExecutableElement) e);
               } else {
                  error(e, "@%s annotation can only be applied to methods", annotationClass.getSimpleName());
               }
            } else {
               error(e, "Annotation @%s requires annotation @%s on the class (possibly inherited)",
                     annotationClass.getSimpleName(), classAnnotationClass.getSimpleName());
            }
         }
      }
   }

   private TypeElement requireType(Element e, Class<? extends Annotation> annotationClass) {
      if (!e.getKind().isClass() && !e.getKind().isInterface()) {
         error(e, "Only classes or interfaces can be annotated with @%s", annotationClass.getSimpleName());
         return null;
      }
      if (e.getModifiers().contains(Modifier.PRIVATE)) {
         error(e, "Component classes must be public or package-private");
      }
      return (TypeElement) e;
   }

   private Scopes getScope(TypeElement typeElement) {
      List<Scope> annotations = getAnnotations(typeElement, Scope.class);
      Scopes scope = null;
      for (Scope annotation : annotations) {
         if (scope == null || scope == Scopes.NONE) {
            scope = annotation.value();
         } else if (annotation.value() != Scopes.NONE && scope != annotation.value()) {
            if (!annotation.value().equals(scope) && annotation.value() != Scopes.NONE) {
               error(typeElement, "Inherited multiple scopes: %s and %s", annotation.value(), scope);
            }
         }
      }
      if (scope == null) {
         throw new IllegalStateException("Scope must be present");
      }
      return scope;
   }

   private boolean getSurvivesRestarts(TypeElement typeElement) {
      boolean survivesRestarts = typeElement.getAnnotation(SurvivesRestarts.class) != null;

      List<SurvivesRestarts> allAnnotations = getAnnotations(typeElement, SurvivesRestarts.class);
      if (!survivesRestarts && !allAnnotations.isEmpty()) {
         error(typeElement, "Inherited @SurvivesRestarts must also be defined on the concrete class");
      }
      return survivesRestarts;
   }

   private CharSequence binaryNameLiteral(TypeElement typeElement) {
      return typeElement == null ? null : "\"" + binaryName(typeElement) + '"';

   }

   private TypeElement getSuperClass(TypeElement typeElement, Class<? extends Annotation> annotationClass) {
      TypeElement superElement = (TypeElement) types().asElement(typeElement.getSuperclass());

      if (superElement == null || superElement.getAnnotation(annotationClass) != null)
         return superElement;

      return getSuperClass(superElement, annotationClass);
   }

   private Collection<TypeElement> getInterfaces(TypeElement typeElement, Class<? extends Annotation> annotationClass) {
      Set<TypeElement> interfaces = new HashSet<>();
      List<? extends TypeMirror> superTypes = types().directSupertypes(typeElement.asType());
      for (TypeMirror type : superTypes) {
         TypeElement superElement = (TypeElement) ((DeclaredType) type).asElement();
         if (superElement.getKind().isInterface() && superElement.getAnnotation(annotationClass) != null)
            interfaces.add(superElement);

         interfaces.addAll(getInterfaces(superElement, annotationClass));
      }

      return interfaces;
   }

   private void error(Element e, String format, Object... params) {
      log(Diagnostic.Kind.ERROR, e, format, params);
      errorReported = true;
   }

   private void warn(Element e, String format, Object... params) {
      log(Diagnostic.Kind.WARNING, e, format, params);
   }

   private void info(Element e, String format, Object... params) {
      log(Diagnostic.Kind.NOTE, e, format, params);
   }

   private void log(Diagnostic.Kind level, Element e, String format, Object... params) {
      String formatted = String.format(format, params);
      if (e != null) {
         processingEnv.getMessager().printMessage(level, formatted, e);
      } else {
         processingEnv.getMessager().printMessage(level, formatted);
      }
   }

   private <A extends Annotation> List<A> getAnnotations(TypeElement clazz, Class<A> annotationClass) {
      List<A> list = new ArrayList<>(2);
      collectAnnotations(list, clazz, annotationClass);
      return list;
   }

   private <A extends Annotation> void collectAnnotations(List<A> list, TypeElement clazz, Class<A> annotationClass) {
      A[] annotations = clazz.getAnnotationsByType(annotationClass);
      Collections.addAll(list, annotations);
      for (TypeMirror supertype : types().directSupertypes(clazz.asType())) {
         collectAnnotations(list, (TypeElement) types().asElement(supertype), annotationClass);
      }
   }

   private Elements elements() {
      return processingEnv.getElementUtils();
   }

   private Types types() {
      return processingEnv.getTypeUtils();
   }

   private Filer filer() {
      return processingEnv.getFiler();
   }

   static class ModuleData {
      CharSequence moduleClassName;
      InfinispanModule moduleAnnotation;
      CharSequence classPrefix;
      CharSequence packageName;
      Map<PackageElement, PackageData> packages = new HashMap<>();
      Map<TypeElement, List<String>> generatedSources;

      ModuleData() {
      }

      void setModuleClass(TypeElement moduleType, CharSequence modulePackageName) {
         this.moduleClassName = moduleType.toString();
         this.packageName = modulePackageName;
         this.moduleAnnotation = moduleType.getAnnotation(InfinispanModule.class);
         this.classPrefix = moduleToClassPrefix(moduleAnnotation.name());
      }

      private CharSequence moduleToClassPrefix(CharSequence moduleName) {
         StringBuilder classPrefix = new StringBuilder();
         for (String element : MODULE_NAME_SEPARATOR_PATTERN.split(moduleName)) {
            classPrefix.append(Character.toUpperCase(element.charAt(0)))
                       .append(element.subSequence(1, element.length()));
         }
         return classPrefix;
      }

      private void addComponent(TypeElement typeElement,
                                         PackageElement packageElement) {
         PackageData packageData = packages.computeIfAbsent(packageElement, PackageData::new);
         packageData.components.computeIfAbsent(typeElement, ComponentData::new);
      }

      private ComponentData getComponent(TypeElement typeElement,
                                         PackageElement packageElement) {
         PackageData packageData = packages.get(packageElement);
         return packageData != null ? packageData.components.get(typeElement) : null;
      }

      Stream<ComponentData> components() {
         return packages.values().stream().flatMap(p -> p.components.values().stream());
      }
   }

   static class PackageData {
      final PackageElement packageElement;
      final Map<TypeElement, ComponentData> components = new HashMap<>();

      PackageData(PackageElement pe) {
         packageElement = pe;
      }
   }

   static class ComponentData {
      final TypeElement typeElement;
      final List<VariableElement> injectFields = new ArrayList<>();
      final List<ExecutableElement> injectMethods = new ArrayList<>(1);
      final List<ExecutableElement> startMethods = new ArrayList<>(1);
      final List<ExecutableElement> postStartMethods = new ArrayList<>(1);
      final List<ExecutableElement> stopMethods = new ArrayList<>(1);

      final List<VariableElement> mbeanAttributeFields = new ArrayList<>();
      final List<ExecutableElement> mbeanAttributeMethods = new ArrayList<>();
      final List<ExecutableElement> mbeanOperations = new ArrayList<>();

      ComponentData(TypeElement typeElement) {
         this.typeElement = typeElement;
      }

      void addInjectField(VariableElement e) {
         injectFields.add(e);
      }

      void addInjectMethod(ExecutableElement e) {
         injectMethods.add(e);
      }

      void addStartMethod(ExecutableElement e) {
         startMethods.add(e);
      }

      void addPostStartMethod(ExecutableElement e) {
         postStartMethods.add(e);
      }

      void addStopMethod(ExecutableElement e) {
         stopMethods.add(e);
      }

      void addManagedAttributeField(VariableElement e) {
         mbeanAttributeFields.add(e);
      }

      void addManagedAttributeMethod(ExecutableElement e) {
         mbeanAttributeMethods.add(e);
      }

      void addManagedOperation(ExecutableElement e) {
         mbeanOperations.add(e);
      }

      boolean hasAccessor() {
         // Don't generate accessor for interfaces
         return typeElement.getKind().isClass() && typeElement.getAnnotation(Scope.class) != null;
      }

      boolean hasDependenciesOrLifecycle() {
         // The class-level annotations define
         return !injectFields.isEmpty() || !injectMethods.isEmpty() || hasLifecycle();
      }

      private boolean hasLifecycle() {
         return !startMethods.isEmpty() || !postStartMethods.isEmpty() || !stopMethods.isEmpty();
      }

      boolean hasMBeanMetadata() {
         // Don't generate MBean metadata for interfaces
         return typeElement.getKind().isClass() && typeElement.getAnnotation(MBean.class) != null;
      }

      Scopes getScope() {
         Scope annotation = typeElement.getAnnotation(Scope.class);
         if (annotation != null) {
            return annotation.value();
         } else
            return null;
      }

      public boolean isFactory() {
         return typeElement.getAnnotation(DefaultFactoryFor.class) != null;
      }
   }
}
