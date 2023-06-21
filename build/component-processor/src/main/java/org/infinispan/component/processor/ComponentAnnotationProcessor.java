package org.infinispan.component.processor;

import static org.infinispan.component.processor.AnnotationTypeValuesExtractor.getTypeValues;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
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
import javax.lang.model.AnnotatedConstruct;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.Name;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.PrimitiveType;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.ElementScanner8;
import javax.lang.model.util.Elements;
import javax.lang.model.util.Types;
import javax.tools.Diagnostic;

import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.factories.annotations.InfinispanModule;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.annotations.SurvivesRestarts;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.jmx.annotations.DataType;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.jmx.annotations.Parameter;
import org.infinispan.jmx.annotations.Units;
import org.kohsuke.MetaInfServices;

@SupportedSourceVersion(SourceVersion.RELEASE_11)
@SupportedAnnotationTypes({ComponentAnnotationProcessor.INFINISPAN_MODULE,
                           ComponentAnnotationProcessor.DEFAULT_FACTORY_FOR,
                           ComponentAnnotationProcessor.SURVIVES_RESTARTS,
                           ComponentAnnotationProcessor.SCOPE,
                           ComponentAnnotationProcessor.MBEAN,
                           ComponentAnnotationProcessor.INJECT,
                           ComponentAnnotationProcessor.START,
                           ComponentAnnotationProcessor.STOP,
                           ComponentAnnotationProcessor.MANAGED_ATTRIBUTE,
                           ComponentAnnotationProcessor.MANAGED_OPERATION,
                           ComponentAnnotationProcessor.MANAGED_OPERATION_PARAMETER})
@MetaInfServices(Processor.class)
public class ComponentAnnotationProcessor extends AbstractProcessor {
   static final String INFINISPAN_MODULE = "org.infinispan.factories.annotations.InfinispanModule";
   static final String DEFAULT_FACTORY_FOR = "org.infinispan.factories.annotations.DefaultFactoryFor";
   static final String SURVIVES_RESTARTS = "org.infinispan.factories.annotations.SurvivesRestarts";
   static final String SCOPE = "org.infinispan.factories.scopes.Scope";
   static final String MBEAN = "org.infinispan.jmx.annotations.MBean";

   static final String INJECT = "org.infinispan.factories.annotations.Inject";
   static final String START = "org.infinispan.factories.annotations.Start";
   static final String STOP = "org.infinispan.factories.annotations.Stop";
   static final String MANAGED_ATTRIBUTE = "org.infinispan.jmx.annotations.ManagedAttribute";
   static final String MANAGED_OPERATION = "org.infinispan.jmx.annotations.ManagedOperation";
   static final String MANAGED_OPERATION_PARAMETER = "org.infinispan.jmx.annotations.Parameter";

   private static final String FACTORY_CLASSES = "classes";
   private static final String COMPONENT_REF_CLASS = "org.infinispan.factories.impl.ComponentRef";
   private static final String AUTO_INSTANTIABLE_FACTORY_CLASS = "org.infinispan.factories.AutoInstantiableFactory";
   private static final Pattern MODULE_NAME_PATTERN = Pattern.compile("[a-zA-Z][-a-zA-Z0-9]*");
   private static final Pattern MODULE_NAME_SEPARATOR_PATTERN = Pattern.compile("-");

   private boolean errorReported = false;
   private ModelBuilder modelBuilder;
   private TypeMirror autoInstantiableType;

   @Override
   public synchronized void init(ProcessingEnvironment processingEnv) {
      super.init(processingEnv);

      modelBuilder = new ModelBuilder();

      try {
         parseGeneratedSources(modelBuilder);
      } catch (Throwable t) {
         uncaughtException(t);
      }
   }

   @Override
   public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
      try {
         TypeElement autoInstantiableElement = elements().getTypeElement(AUTO_INSTANTIABLE_FACTORY_CLASS);
         if (autoInstantiableElement == null) {
            error(null, "Could not find type %s in the classpath, please rebuild", AUTO_INSTANTIABLE_FACTORY_CLASS);
            return false;
         }

         autoInstantiableType = autoInstantiableElement.asType();
         modelBuilder.scan(roundEnv.getRootElements(), null);

         // In theory we could generate the package classes early (or better generate a class for each component)
         // but the classes we generate don't have any annotations for other processors to parse.
         if (roundEnv.processingOver()) {
            Model model = modelBuilder.getModel();
            if (modelBuilder.module == null) {
               error(null, "@InfinispanModule annotation not found in any class, please perform a clean build. " +
                           "Found components %s", modelBuilder.annotatedTypes.keySet());
               return true;
            }

            // Only generate classes if valid
            if (errorReported || roundEnv.errorRaised())
               return true;

            Generator generator = new Generator(model, filer());

            generator.writePackageClasses();

            // IntelliJ will delete the generated file before compilation if any of the source elements has changed
            // We make the module impl and service fine depend only on the module class,
            // so we know either they are present on disk or the module class is being compiled.
            TypeElement[] sourceElements = {model.module.typeElement};
            generator.writeModuleClass(sourceElements);
            generator.writeServiceFile(sourceElements);
         }
      } catch (Throwable t) {
         uncaughtException(t);
      }
      return true;
   }

   private void parseGeneratedSources(ModelBuilder modelBuilder) throws IOException {
      // Set an empty default to simplify early returns
      modelBuilder.parsedTypes = Collections.emptyMap();

      String moduleImplementationName = Generator.readServiceFile(filer());
      if (moduleImplementationName == null) {
         return;
      }

      Map.Entry<String, Set<String>> moduleClassAndPackages =
            Generator.readModuleClass(filer(), moduleImplementationName);
      if (moduleClassAndPackages == null) {
         info(null, "Ignoring removed or renamed module implementation %s", moduleImplementationName);
         return;
      }

      TypeElement moduleClassElement = elements().getTypeElement(moduleClassAndPackages.getKey());
      InfinispanModule moduleAnnotation = moduleClassElement != null ?
                                          getAnnotation(moduleClassElement, InfinispanModule.class) : null;
      if (moduleAnnotation == null) {
         error(null, "Ignoring invalid module implementation %s", moduleImplementationName);
         return;
      }

      String classPrefix = moduleToClassPrefix(moduleAnnotation.name());

      Set<String> packages = moduleClassAndPackages.getValue();
      Map<String, Model.ParsedType> parsedTypes = new HashMap<>();
      for (String packageName : packages) {
         Map<String, List<String>> packageSources = Generator.readPackageClass(filer(), packageName, classPrefix);
         if (packageSources == null) {
            info(null, "Ignoring removed or renamed package implementation %s", packageName);
            continue;
         }
         packageSources.forEach((qualifiedName, code) -> {
            TypeElement sourceType = elements().getTypeElement(qualifiedName);
            if (sourceType == null) {
               info( null, "Ignoring removed component class %s", qualifiedName);
            } else {
               parsedTypes.put(qualifiedName, new Model.ParsedType(sourceType, qualifiedName, packageName, code));
            }
         });
      }

      modelBuilder.parsedTypes = parsedTypes;
      modelBuilder.setModuleClass(moduleClassElement, moduleAnnotation, classPrefix);
   }

   private <A extends Annotation> A getAnnotation(AnnotatedConstruct moduleClassElement, Class<A> annotationType) {
      try {
         return moduleClassElement.getAnnotation(annotationType);
      } catch (ClassCastException e) {
         // The annotation has unresolved values
         // java.lang.ClassCastException: class com.sun.tools.javac.code.Attribute$UnresolvedClass
         // cannot be cast to class com.sun.tools.javac.code.Attribute$Class
         // The code doesn't compile anyway, so we can ignore the annotation.
         return null;
      }
   }

   private static String extractAttributeName(String setterOrGetter) {
      String name = null;

      if (setterOrGetter.startsWith("set") || setterOrGetter.startsWith("get"))
         name = setterOrGetter.substring(3);
      else if (setterOrGetter.startsWith("is"))
         name = setterOrGetter.substring(2);

      if (name != null && name.length() > 1) {
         return Character.toLowerCase(name.charAt(0)) + name.substring(1);
      }

      return null;
   }

   private void uncaughtException(Throwable t) {
      String stackTrace = Stream.of(t.getStackTrace())
                                .map(Object::toString)
                                .collect(Collectors.joining("\n\tat ", "\tat ", ""));
      error(null, "ComponentAnnotationProcessor unexpected error: %s\n%s", t, stackTrace);
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

   private Elements elements() {
      return processingEnv.getElementUtils();
   }

   private Types types() {
      return processingEnv.getTypeUtils();
   }

   private Filer filer() {
      return processingEnv.getFiler();
   }


   private String moduleToClassPrefix(CharSequence moduleName) {
      StringBuilder classPrefix = new StringBuilder();
      for (String element : MODULE_NAME_SEPARATOR_PATTERN.split(moduleName)) {
         classPrefix.append(Character.toUpperCase(element.charAt(0)))
                    .append(element.subSequence(1, element.length()));
      }
      return classPrefix.toString();
   }

   private class ModelBuilder extends ElementScanner8<Void, Void> {
      private Model.Module module;
      Map<String, Model.ParsedType> parsedTypes;

      private final Map<String, Model.AnnotatedType> annotatedTypes = new HashMap<>();
      private Model.AnnotatedType currentType;

      @Override
      public Void visitType(TypeElement e, Void ignored) {
         Model.AnnotatedType savedType = currentType;
         String qualifiedName = e.getQualifiedName().toString();

         InfinispanModule moduleAnnotation = getAnnotation(e, InfinispanModule.class);
         if (moduleAnnotation != null) {
            String classPrefix = moduleToClassPrefix(moduleAnnotation.name());
            setModuleClass(e, moduleAnnotation, classPrefix);
         }

         Scope scope = getAnnotation(e, Scope.class);
         MBean mbean = getAnnotation(e, MBean.class);
         boolean survivesRestarts = getAnnotation(e, SurvivesRestarts.class) != null;

         if (!survivesRestarts) {
            requireAnnotationOnSubType(e, SurvivesRestarts.class);
         }

         if (scope != null || mbean != null) {
            String binaryName = binaryName(e);
            String packageName = elements().getPackageOf(e).getQualifiedName().toString();
            Model.AnnotatedType annotatedType = new Model.AnnotatedType(e, qualifiedName, binaryName, packageName);
            annotatedTypes.put(qualifiedName, annotatedType);
            currentType = annotatedType;

            if (scope != null) {
               validatePackagePrivate(e, Scope.class);
               requireSameScope(e, scope);

               List<String> factoryComponentNames = getFactoryComponentNames(e);
               boolean autoInstantiable = types().isAssignable(e.asType(), autoInstantiableType);
               String superBinaryName = binaryName(getSuperClass(e, Scope.class));
               annotatedType.component = new Model.Component(scope, survivesRestarts, factoryComponentNames,
                                                             autoInstantiable, superBinaryName);
            } else {
               requireAnnotationOnSubType(e, Scope.class);
            }
            if (mbean != null) {
               String superMComponent = binaryName(getSuperClass(e, MBean.class));
               annotatedType.mComponent = new Model.MComponent(mbean, superMComponent);
            } else {
               requireAnnotationOnSubType(e, MBean.class);
            }
         }

         // Ignore the parsed code
         parsedTypes.remove(qualifiedName);

         try {
            return super.visitType(e, ignored);
         } finally {
            currentType = savedType;
         }
      }

      private void setModuleClass(TypeElement e, InfinispanModule moduleAnnotation, String classPrefix) {
         validateModule(e, moduleAnnotation);

         String modulePackageName = elements().getPackageOf(e).getQualifiedName().toString();
         module = new Model.Module(moduleAnnotation, e, modulePackageName, classPrefix);
      }

      private void validateModule(TypeElement e, InfinispanModule moduleAnnotation) {
         String moduleName = moduleAnnotation.name();
         if (!MODULE_NAME_PATTERN.matcher(moduleName).matches()) {
            error(e, "@InfinispanModule name attribute must include only letters, digits or `-`");
         }

         Name moduleClassName = e.getQualifiedName();
         if (module != null && !moduleClassName.contentEquals(module.moduleClassName)) {
            error(e, "@InfinispanModule allowed on a single class, already present on %s",
                  module.moduleClassName);
         }
      }

      @Override
      public Void visitExecutable(ExecutableElement e, Void ignored) {
         String methodName = e.getSimpleName().toString();
         if (getAnnotation(e, Inject.class) != null) {
            validatePackagePrivate(e, Scope.class);
            if (isValidComponent(e, Inject.class)) {
               List<Model.InjectField> parameters = e.getParameters().stream()
                                                     .map(this::makeInjectField)
                                                     .collect(Collectors.toList());
               currentType.component.injectMethods.add(new Model.InjectMethod(methodName, parameters));
            }
         }
         addLifecycleMethod(e, Start.class, c -> c.startMethods, Start::priority);
         addLifecycleMethod(e, Stop.class, c -> c.stopMethods, Stop::priority);

         if (getAnnotation(e, ManagedAttribute.class) != null && isValidMComponent(e, ManagedAttribute.class)) {
            ManagedAttribute attribute = getAnnotation(e, ManagedAttribute.class);
            String name = attribute != null ? attribute.name() : "";
            if (name.isEmpty()) {
               name = extractAttributeName(methodName);
            }
            boolean is = methodName.startsWith("is");
            String type;
            String boxedType;
            if (methodName.startsWith("set")) {
               TypeMirror t = e.getParameters().get(0).asType();
               type = types().erasure(t).toString();
               boxedType = t.getKind().isPrimitive() ? types().boxedClass((PrimitiveType) t).toString() : type;
            } else if (methodName.startsWith("get") || is) {
               TypeMirror t = e.getReturnType();
               type = types().erasure(t).toString();
               boxedType = t.getKind().isPrimitive() ? types().boxedClass((PrimitiveType) t).toString() : type;
            } else {
               error(e, "Method annotated with @ManagedAttribute does not start with `set`, `get`, or `is`");
               type = boxedType = "";
            }
            validateUnits(e, attribute);
            currentType.mComponent.attributes.add(new Model.MAttribute(name, methodName, attribute, true, type, boxedType, is));
         }

         if (getAnnotation(e, ManagedOperation.class) != null)
            if (isValidMComponent(e, ManagedOperation.class)) {
               ManagedOperation operation = getAnnotation(e, ManagedOperation.class);
               List<Model.MParameter> parameters = new ArrayList<>();
               for (VariableElement parameter : e.getParameters()) {
                  Parameter parameterAnnotation = getAnnotation(parameter, Parameter.class);
                  String name = (parameterAnnotation != null && !parameterAnnotation.name().isEmpty()) ?
                                parameterAnnotation.name() : parameter.getSimpleName().toString();
                  String type = types().erasure(parameter.asType()).toString();
                  String description = parameterAnnotation != null ?
                                       parameterAnnotation.description() : null;
                  parameters.add(new Model.MParameter(name, type, description));
               }
               currentType.mComponent.operations.add(
                  new Model.MOperation(methodName, operation, types().erasure(e.getReturnType()).toString(), parameters));
            }
         return super.visitExecutable(e, ignored);
      }

      private void validateUnits(Element e, ManagedAttribute attribute) {
         if (attribute.dataType() == DataType.TIMER) {
            if (!Units.TIME_UNITS.contains(attribute.units())) {
               error(e, "@ManagedAttribute.units is expected to be a time unit since `dataType` is DataType.TIMER");
            }
         }
      }

      private <A extends Annotation>
      void addLifecycleMethod(ExecutableElement e, Class<A> annotationType,
                              Function<Model.Component, List<Model.LifecycleMethod>> methodsExtractor,
                              Function<A, Integer> priorityExtractor) {
         A annotation = getAnnotation(e, annotationType);
         if (annotation == null)
            return;

         if (isValidComponent(e, annotationType)) {
            validateLifecycleMethod(e, annotationType);

            List<Model.LifecycleMethod> methodList = methodsExtractor.apply(currentType.component);
            Integer priority = priorityExtractor.apply(annotation);
            methodList.add(new Model.LifecycleMethod(e.getSimpleName().toString(), priority));
         }
      }

      private <A extends Annotation>
      void validateLifecycleMethod(ExecutableElement method, Class<A> annotationType) {
         validatePackagePrivate(method, annotationType);

         if (!method.getParameters().isEmpty()) {
            error(method, "Methods annotated @%s must have no parameters", annotationType.getSimpleName());
         }

         if (existsInSuper(method, annotationType)) {
            error(method, "Inherited lifecycle methods should not be annotated with @%s",
                  annotationType.getSimpleName());
         }
      }

      private <A extends Annotation> boolean existsInSuper(ExecutableElement method, Class<A> annotationType) {
         TypeElement superAccessorType = getSuperClass(currentType.typeElement, Scope.class);
         while (superAccessorType != null) {
            for (Element element : superAccessorType.getEnclosedElements()) {
               if (element.getKind() == ElementKind.METHOD &&
                   element.getSimpleName().contentEquals(method.getSimpleName()) &&
                   getAnnotation(element, annotationType) != null) {
                  // Method is already included in the super class lifecycle, ignore it
                  return true;
               }
            }

            superAccessorType = getSuperClass(superAccessorType, Scope.class);
         }
         return false;
      }

      @Override
      public Void visitVariable(VariableElement e, Void ignored) {
         if (getAnnotation(e, Inject.class) != null) {
            validatePackagePrivate(e, Scope.class);

            if (isValidComponent(e, Inject.class)) {
               currentType.component.injectFields.add(makeInjectField(e));
            }
         }

         ManagedAttribute attribute = getAnnotation(e, ManagedAttribute.class);
         if (attribute != null) {
            if (isValidMComponent(e, ManagedAttribute.class)) {
               String name = e.getSimpleName().toString();
               TypeMirror t =  e.asType();
               String type = types().erasure(t).toString();
               String boxedType = t.getKind().isPrimitive() ? types().boxedClass((PrimitiveType) t).toString() : type;
               validateUnits(e, attribute);
               currentType.mComponent.attributes.add(new Model.MAttribute(name, name, attribute, false, type, boxedType, false));
            }
         }

         return super.visitVariable(e, ignored);
      }

      private Model.InjectField makeInjectField(VariableElement e) {
         String fieldName = e.getSimpleName().toString();
         TypeMirror componentType = getInjectedType(e);
         String binaryName = binaryName(componentType);
         String componentName = getComponentName(e, binaryName);
         String qualifiedTypeName = componentType.toString();
         boolean isComponentRef = isComponentRef(e);
         return new Model.InjectField(fieldName, qualifiedTypeName, componentName, isComponentRef);
      }

      private void requireSameScope(TypeElement e, Scope scope) {
         for (TypeMirror superType : types().directSupertypes(e.asType())) {
            Element superTypeElement = ((DeclaredType) superType).asElement();
            Scope superScope = getAnnotation(superTypeElement, Scope.class);
            if (superScope != null && superScope.value() != Scopes.NONE) {
               if (scope.value() != superScope.value()) {
                  error(e, "Scope declared on class %s (%s) does not match scope inherited from %s (%s)",
                        e.getSimpleName(), scope.value(), superTypeElement.getSimpleName(), superScope.value());
               }
            }
         }
      }

      private boolean isValidComponent(Element methodOrField, Class<? extends Annotation> methodAnnotation) {

         boolean valid = true;
         if (currentType == null || currentType.component == null) {
            error(methodOrField, "When a method is annotated with @%s, the type must be annotated with @%s",
                  methodAnnotation.getSimpleName(), Scope.class.getSimpleName());
            valid = false;
         }
         if (!methodOrField.getEnclosingElement().getKind().isClass()) {
            error(methodOrField, "Interface methods must not be annotated with @%s", methodAnnotation.getSimpleName());
            valid = false;
         }
         return valid;
      }

      private boolean isValidMComponent(Element methodOrField, Class<? extends Annotation> methodAnnotation) {
         boolean valid = true;
         // ResourceDMBean still uses reflection, so it doesn't need package-private

         if (currentType == null || currentType.mComponent == null) {
            error(methodOrField, "When a method is annotated with @%s, the class must be annotated with @%s",
                  methodAnnotation.getSimpleName(), MBean.class.getSimpleName());
            valid = false;
         }

         if (!methodOrField.getEnclosingElement().getKind().isClass()) {
            error(methodOrField, "Interface methods must not be annotated with @%s", methodAnnotation.getSimpleName());
            valid = false;
         }
         return valid;
      }

      private void validatePackagePrivate(Element element, Class<? extends Annotation> annotationType) {
         if (element.getModifiers().contains(Modifier.PRIVATE)) {
            String kind;
            if (element.getKind().isField()) {
               kind = "Fields";
            } else if (element.getKind() == ElementKind.METHOD) {
               kind = "Methods";
            } else {
               kind = "Types";
            }
            error(element, "%s annotated with @%s must not be private", kind, annotationType);
         }
      }

      private void requireAnnotationOnSubType(TypeElement typeElement, Class<? extends Annotation> typeAnnotation) {
         for (TypeMirror supertype : types().directSupertypes(typeElement.asType())) {
            TypeElement superTypeElement = (TypeElement) ((DeclaredType) supertype).asElement();
            if (getAnnotation(superTypeElement, typeAnnotation) != null) {
               error(typeElement, "Type %s must have annotation @%s because it extends/implements %s. " +
                                  "Note that interface annotations are not inherited",
                     typeElement.getSimpleName(), typeAnnotation.getSimpleName(), superTypeElement.getSimpleName());
            }
         }
      }

      public Model getModel() {
         Map<String, Model.Package> packages = new HashMap<>();
         for (Model.AnnotatedType annotatedType : annotatedTypes.values()) {
            Model.Package aPackage = packages.computeIfAbsent(annotatedType.packageName, Model.Package::new);
            aPackage.annotatedTypes.add(annotatedType);
            aPackage.typeElements.add(annotatedType.typeElement);
         }
         for (Model.ParsedType parsedType : parsedTypes.values()) {
            Model.Package aPackage = packages.computeIfAbsent(parsedType.packageName, Model.Package::new);
            aPackage.parsedTypes.add(parsedType);
            aPackage.typeElements.add(parsedType.typeElement);
         }

         return new Model(module, annotatedTypes, parsedTypes, packages);
      }

      private List<String> getFactoryComponentNames(TypeElement typeElement) {
         DefaultFactoryFor factoryAnnotation = getAnnotation(typeElement, DefaultFactoryFor.class);
         List<String> componentNames = new ArrayList<>();
         if (factoryAnnotation != null) {
            TypeElement defaultFactoryForType = elements().getTypeElement(DefaultFactoryFor.class.getName());
            List<TypeMirror> producedTypes = getTypeValues(typeElement, defaultFactoryForType, FACTORY_CLASSES);
            for (TypeMirror type : producedTypes) {
               componentNames.add(binaryName(type));
            }
            Collections.addAll(componentNames, factoryAnnotation.names());
         }
         return componentNames;
      }

      private String binaryName(TypeElement typeElement) {
         if (typeElement == null)
            return null;

         return elements().getBinaryName(typeElement).toString();
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

      private String getComponentName(VariableElement parameter, String componentTypeName) {
         ComponentName nameAnnotation = getAnnotation(parameter, ComponentName.class);
         return nameAnnotation != null ? nameAnnotation.value() : componentTypeName;
      }

      private boolean isComponentRef(VariableElement injectFieldOrParameter) {
         String erasedType = types().erasure(injectFieldOrParameter.asType()).toString();
         return COMPONENT_REF_CLASS.equals(erasedType);
      }

      private String binaryName(TypeMirror type) {
         return binaryName((TypeElement) types().asElement(type));
      }

      private TypeElement getSuperClass(TypeElement typeElement, Class<? extends Annotation> annotationClass) {
         TypeElement superElement = (TypeElement) types().asElement(typeElement.getSuperclass());

         if (superElement == null || getAnnotation(superElement, annotationClass) != null)
            return superElement;

         return getSuperClass(superElement, annotationClass);
      }
   }
}
