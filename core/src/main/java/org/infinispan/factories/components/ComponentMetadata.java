package org.infinispan.factories.components;

import java.io.Serializable;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.PostStart;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.annotations.SurvivesRestarts;
import org.infinispan.factories.scopes.Scope;

/**
 * This class contains all of the metadata and implications expressed via the {@link Scope}, {@link SurvivesRestarts},
 * {@link DefaultFactoryFor}, {@link ComponentName}, {@link Inject}, {@link Start} and {@link Stop} annotations.  Instead
 * of scanning for these annotations and working out dependency chains at runtime "on-demand", since Infinispan 5.1, this
 * process now happens offline, at build-time.
 * <p />
 * When compiling Infinispan, components and their dependency chains are inspected and the information expressed by the
 * annotations above are denormalized and a series of {@link ComponentMetadata} objects are created and persisted in the
 * Infinispan jar.
 * <p />
 * This metadata is then read in by the {@link ComponentMetadataRepo} at runtime, and used by the {@link ComponentRegistry}
 * and other factory-like classes to bootstrap an Infinispan node.
 * <p />
 * Also see {@link ManageableComponentMetadata} for components that also expose JMX information.

 * @author Manik Surtani
 * @since 5.1
 * @see ManageableComponentMetadata
 * @see ComponentMetadataRepo
 */
public class ComponentMetadata implements Serializable {
   private static final InjectFieldMetadata[] EMPTY_INJECT_FIELDS = {};
   public static final InjectMetadata[] EMPTY_INJECT_METHODS = {};
   public static final PrioritizedMethodMetadata[] EMPTY_PRIORITIZED_METHODS = {};

   private String name;
   private transient Map<String, String> dependencies;
   private InjectFieldMetadata[] injectFields;
   private InjectMetadata[] injectMetadata;
   private PrioritizedMethodMetadata[] startMethods;
   private PrioritizedMethodMetadata[] postStartMethods;
   private PrioritizedMethodMetadata[] stopMethods;
   private boolean globalScope = false;
   private boolean survivesRestarts = false;
   private transient Class<?> clazz;

   ComponentMetadata() {
      globalScope = false;
      survivesRestarts = true;
   }

   public ComponentMetadata(Class<?> component, List<Field> injectFields, List<Method> injectMethods, List<Method> startMethods,
                            List<Method> postStartMethods, List<Method> stopMethods, boolean global,
                            boolean survivesRestarts) {
      clazz = component;
      name = component.getName();
      globalScope = global;
      this.survivesRestarts = survivesRestarts;

      if (startMethods != null && !startMethods.isEmpty()) {
         this.startMethods = new PrioritizedMethodMetadata[startMethods.size()];
         int i=0;
         for (Method m : startMethods) {
            Start s = m.getAnnotation(Start.class);
            this.startMethods[i++] = new PrioritizedMethodMetadata(m.getName(), s.priority());
         }
      }

      if (postStartMethods != null && !postStartMethods.isEmpty()) {
         this.postStartMethods = new PrioritizedMethodMetadata[postStartMethods.size()];
         int i=0;
         for (Method m : postStartMethods) {
            PostStart s = m.getAnnotation(PostStart.class);
            this.postStartMethods[i++] = new PrioritizedMethodMetadata(m.getName(), s.priority());
         }
      }

      if (stopMethods != null && !stopMethods.isEmpty()) {
         this.stopMethods = new PrioritizedMethodMetadata[stopMethods.size()];
         int i=0;
         for (Method m : stopMethods) {
            Stop s = m.getAnnotation(Stop.class);
            this.stopMethods[i++] = new PrioritizedMethodMetadata(m.getName(), s.priority());
         }
      }

      int deps = (injectFields == null ? 0 : injectFields.size()) + (injectMethods == null ? 0 : injectMethods.size());
      if (deps > 0) {
         this.dependencies = new HashMap<>(deps * 2);
      }

      if (injectFields != null && !injectFields.isEmpty()) {
         this.injectFields = new InjectFieldMetadata[injectFields.size()];
         int j = 0;
         for (Field f : injectFields) {
            ComponentName[] cn = f.getAnnotationsByType(ComponentName.class);
            String componentType = f.getType().getName();
            String componentName = null;
            if (cn.length > 1) {
               throw new IllegalStateException("Only one name expected");
            } else if (cn.length == 1) {
               componentName = cn[0].value();
            }
            if (componentName == null) {
               dependencies.put(componentType, componentType);
            } else {
               dependencies.put(componentName, componentType);
            }

            this.injectFields[j++] = new InjectFieldMetadata(
                  f.getDeclaringClass().getName(), f.getName(), componentType, componentName);
         }
      }

      if (injectMethods != null && !injectMethods.isEmpty()) {
         this.injectMetadata = new InjectMetadata[injectMethods.size()];
         int j=0;
         for (Method m : injectMethods) {

            InjectMetadata injectMetadata = new InjectMetadata(m.getName());

            Class<?>[] parameterTypes = m.getParameterTypes();

            // Need to use arrays instead of Map due to JDK bug see ISPN-3611
            String[] params = new String[parameterTypes.length];
            String[] paramNames = new String[parameterTypes.length];

            // Add this to our dependencies map
            Annotation[][] annotations = m.getParameterAnnotations();
            for (int i=0; i<parameterTypes.length; i++) {
               String componentName = findComponentName(annotations, i);
               String parameterType = parameterTypes[i].getName();
               params[i] = parameterType;
               if (componentName == null) {
                  dependencies.put(parameterType, parameterType);
               } else {
                  paramNames[i] = componentName;
                  dependencies.put(componentName, parameterType);
               }
            }
            injectMetadata.parameters = params;
            injectMetadata.parameterNames = paramNames;
            this.injectMetadata[j++] = injectMetadata;
         }
      }
   }

   private String findComponentName(Annotation[][] annotations, int position) {
      if (annotations != null && annotations.length > position) {
         Annotation[] paramAnnotations = annotations[position];
         if (paramAnnotations != null) {
            for (Annotation a: paramAnnotations) {
               if (a instanceof ComponentName) {
                  return ((ComponentName) a).value();
               }
            }
         }
      }
      return null;
   }

   public String getName() {
      return name;
   }

   public Map<String, String> getDependencies() {
      return dependencies;
   }

   public InjectFieldMetadata[] getInjectFields() {
      return injectFields == null ? EMPTY_INJECT_FIELDS : injectFields;
   }

   public InjectMetadata[] getInjectMethods() {
      if (injectMetadata == null) return EMPTY_INJECT_METHODS;
      return injectMetadata;
   }

   public PrioritizedMethodMetadata[] getStartMethods() {
      if (startMethods == null) return EMPTY_PRIORITIZED_METHODS;
      return startMethods;
   }

   public PrioritizedMethodMetadata[] getPostStartMethods() {
      if (postStartMethods == null) return EMPTY_PRIORITIZED_METHODS;
      return postStartMethods;
   }

   public PrioritizedMethodMetadata[] getStopMethods() {
      if (stopMethods == null) return EMPTY_PRIORITIZED_METHODS;
      return stopMethods;
   }

   public boolean isGlobalScope() {
      return globalScope;
   }

   public boolean isSurvivesRestarts() {
      return survivesRestarts;
   }

   public boolean isManageable() {
      return false;
   }

   public Class<?> getClazz() {
      return clazz;
   }

   public ManageableComponentMetadata toManageableComponentMetadata() {
      throw new UnsupportedOperationException("This component is not manageable!");
   }

   @Override
   public String toString() {
      return "ComponentMetadata{" +
            "name='" + name + '\'' +
            ", dependencies=" + dependencies +
            ", injectMetadata=" + Arrays.toString(injectMetadata) +
            ", startMethods=" + Arrays.toString(startMethods) +
            ", postStartMethods=" + Arrays.toString(postStartMethods) +
            ", stopMethods=" + Arrays.toString(stopMethods) +
            ", globalScope=" + globalScope +
            ", survivesRestarts=" + survivesRestarts +
            '}';
   }

   /**
    * This class encapsulates metadata on a prioritized method, such as one annotated with {@link Start} or {@link @Stop}
    */
   public static class PrioritizedMethodMetadata implements Serializable {
      String methodName;
      transient Method method;
      int priority;

      public PrioritizedMethodMetadata(String methodName, int priority) {
         this.methodName = methodName;
         this.priority = priority;
      }

      public String getMethodName() {
         return methodName;
      }

      public Method getMethod() {
         return method;
      }

      public void setMethod(Method method) {
         this.method = method;
      }

      public int getPriority() {
         return priority;
      }

      @Override
      public String toString() {
         return method.getName() + "(priority=" + priority + ")";
      }
   }

   /**
    * This class encapsulates metadata on an inject method, such as one annotated with {@link Inject}
    */
   public static class InjectMetadata implements Serializable {

      //To avoid mismatches during development like as created by Maven vs IDE compiled classes:
      private static final long serialVersionUID = 4848856551345751894L;

      String methodName;
      transient Method method;
      String[] parameters;
      transient Class<?>[] parameterClasses;
      String[] parameterNames;

      private InjectMetadata(String methodName) {
         this.methodName = methodName;
      }

      public String getMethodName() {
         return methodName;
      }

      public String[] getParameters() {
         return parameters;
      }

      public String getParameterName(int subscript) {
         String name = parameterNames == null ? null : parameterNames[subscript];
         return name == null ? parameters[subscript] : name;
      }

      public boolean isParameterNameSet(int subscript) {
         return parameterNames != null && parameterNames[subscript] != null;
      }

      public synchronized Method getMethod() {
         return method;
      }

      public synchronized void setMethod(Method method) {
         this.method = method;
      }

      public synchronized Class<?>[] getParameterClasses() {
         return parameterClasses;
      }

      public synchronized void setParameterClasses(Class<?>[] parameterClasses) {
         this.parameterClasses = parameterClasses;
      }

      @Override
      public String toString() {
         return methodName + "(" + String.join(", ", parameters) + ")";
      }
   }

   public class InjectFieldMetadata implements Serializable {
      private static final long serialVersionUID = 7523698423843422884L;
      private final String fieldClassName;
      private final String fieldName;
      private final String componentName;
      private final String componentType;
      private transient Field field;
      private transient Class<?> componentClass;

      public InjectFieldMetadata(String fieldClassName, String fieldName, String componentType, String componentName) {
         this.fieldClassName = fieldClassName;
         this.fieldName = fieldName;
         this.componentName = componentName;
         this.componentType = componentType;
      }

      public Field getField() {
         return field;
      }

      public void setField(Field field) {
         this.field = field;
      }

      public String getFieldClassName() {
         return fieldClassName;
      }

      public String getFieldName() {
         return fieldName;
      }

      public Class<?> getComponentClass() {
         return componentClass;
      }

      public void setComponentClass(Class<?> componentClass) {
         this.componentClass = componentClass;
      }

      public String getComponentType() {
         return componentType;
      }

      public String getComponentName() {
         return componentName;
      }
   }
}
