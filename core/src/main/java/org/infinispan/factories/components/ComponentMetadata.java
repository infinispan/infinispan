package org.infinispan.factories.components;

import java.io.Serializable;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
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
import org.infinispan.factories.impl.ComponentRef;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;

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
   private static final long serialVersionUID = 0xC0119011E7ADA7AL;
   private static final InjectFieldMetadata[] EMPTY_INJECT_FIELDS = {};
   public static final InjectMethodMetadata[] EMPTY_INJECT_METHODS = {};
   public static final PrioritizedMethodMetadata[] EMPTY_PRIORITIZED_METHODS = {};

   private String name;
   private transient Map<String, String> dependencies;
   private InjectFieldMetadata[] injectFields;
   private InjectMethodMetadata[] injectMethodMetadata;
   private PrioritizedMethodMetadata[] startMethods;
   private PrioritizedMethodMetadata[] postStartMethods;
   private PrioritizedMethodMetadata[] stopMethods;
   private Scopes scope;
   private boolean survivesRestarts;
   transient volatile Class<?> clazz;

   ComponentMetadata() {
      survivesRestarts = true;
   }

   public ComponentMetadata(Class<?> component, List<Field> injectFields, List<Method> injectMethods, List<Method> startMethods,
                            List<Method> postStartMethods, List<Method> stopMethods, boolean global,
                            boolean survivesRestarts) {
      this(component, injectFields, injectMethods, startMethods, postStartMethods, stopMethods,
           global ? Scopes.GLOBAL : Scopes.NAMED_CACHE, survivesRestarts);
   }

   public ComponentMetadata(Class<?> component, List<Field> injectFields, List<Method> injectMethods,
                            List<Method> startMethods,
                            List<Method> postStartMethods, List<Method> stopMethods, Scopes scope,
                            boolean survivesRestarts) {
      clazz = component;
      name = component.getName();
      this.scope = scope;
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
            boolean lazy = f.getType() == ComponentRef.class;
            ComponentName[] cn = f.getAnnotationsByType(ComponentName.class);
            String componentType = extractDependencyType(f.getType(), f.getGenericType(), lazy);
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
               f.getDeclaringClass().getName(), f.getName(), componentType, componentName, lazy);
         }
      }

      if (injectMethods != null && !injectMethods.isEmpty()) {
         this.injectMethodMetadata = new InjectMethodMetadata[injectMethods.size()];
         int j=0;
         for (Method m : injectMethods) {
            InjectMethodMetadata injectMethodMetadata = new InjectMethodMetadata(m.getName());

            Class<?>[] parameterTypes = m.getParameterTypes();
            Type[] genericParameterTypes = m.getGenericParameterTypes();

            // Need to use arrays instead of Map due to JDK bug see ISPN-3611
            String[] params = new String[parameterTypes.length];
            String[] paramNames = new String[parameterTypes.length];
            boolean[] paramLazy = new boolean[parameterTypes.length];

            // Add this to our dependencies map
            Annotation[][] annotations = m.getParameterAnnotations();
            for (int i=0; i<parameterTypes.length; i++) {
               String componentName = findComponentName(annotations, i);
               boolean lazy = parameterTypes[i] == ComponentRef.class;
               paramLazy[i] = lazy;
               String parameterType = extractDependencyType(parameterTypes[i], genericParameterTypes[i], lazy);
               params[i] = parameterType;
               if (componentName == null) {
                  dependencies.put(parameterType, parameterType);
               } else {
                  paramNames[i] = componentName;
                  dependencies.put(componentName, parameterType);
               }
            }
            injectMethodMetadata.parameters = params;
            injectMethodMetadata.parameterNames = paramNames;
            injectMethodMetadata.parameterLazy = paramLazy;
            this.injectMethodMetadata[j++] = injectMethodMetadata;
         }
      }
   }

   public String extractDependencyType(Type erasedType, Type genericType, boolean lazy) {
      String componentType;
      if (lazy) {
         Type actualComponentType = ((ParameterizedType) genericType).getActualTypeArguments()[0];
         if (actualComponentType instanceof ParameterizedType) {
            // Ignore any generic parameters on the component type
            componentType = ((ParameterizedType) actualComponentType).getRawType().getTypeName();
         } else {
            componentType = actualComponentType.getTypeName();
         }
      } else {
         componentType = erasedType.getTypeName();
      }
      return componentType;
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

   public InjectMethodMetadata[] getInjectMethods() {
      if (injectMethodMetadata == null) return EMPTY_INJECT_METHODS;
      return injectMethodMetadata;
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
      return scope == Scopes.GLOBAL;
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
             ", injectMethods=" + Arrays.toString(injectMethodMetadata) +
             ", startMethods=" + Arrays.toString(startMethods) +
             ", postStartMethods=" + Arrays.toString(postStartMethods) +
             ", stopMethods=" + Arrays.toString(stopMethods) +
             ", scope=" + scope +
             ", survivesRestarts=" + survivesRestarts +
             '}';
   }

   public Scopes getScope() {
      return scope;
   }

   /**
    * This class encapsulates metadata on a prioritized method, such as one annotated with {@link Start} or {@link Stop}
    */
   public static class PrioritizedMethodMetadata implements Serializable {
      private static final long serialVersionUID = 0x21210121712EDL;
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
         StringBuilder sb = new StringBuilder();
         sb.append(methodName);
         if (method != null) {
            sb.append("(");
            boolean comma = false;
            for (Class<?> paramClass : method.getParameterTypes()) {
               if (comma) {
                  sb.append(", ");
               } else {
                  comma = true;
               }
               sb.append(paramClass.getName());
            }
            sb.append(")");
         }
         sb.append("(priority=").append(priority).append(")");
         return sb.toString();
      }
   }

   /**
    * This class encapsulates metadata on an inject method, such as one annotated with {@link Inject}
    */
   public static class InjectMethodMetadata implements Serializable {

      //To avoid mismatches during development like as created by Maven vs IDE compiled classes:
      private static final long serialVersionUID = 3662286908891061057L;

      String methodName;
      transient Method method;
      String[] parameters;
      transient Class<?>[] parameterClasses;
      String[] parameterNames;
      boolean[] parameterLazy;

      private InjectMethodMetadata(String methodName) {
         this.methodName = methodName;
      }

      public String getMethodName() {
         return methodName;
      }

      public String[] getParameters() {
         return parameters;
      }

      /**
       * @deprecated Singe 9.4, please use {@link #getDependencyName(int)} instead.
       */
      @Deprecated
      public String getParameterName(int subscript) {
         return getDependencyName(subscript);
      }

      public String getDependencyName(int subscript) {
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

      public boolean getParameterLazy(int i) {
         return parameterLazy[i];
      }

      @Override
      public String toString() {
         return methodName + "(" + String.join(", ", parameters) + ")";
      }
   }

   public class InjectFieldMetadata implements Serializable {
      private static final long serialVersionUID = 1040295625623725061L;
      private final String fieldClassName;
      private final String fieldName;
      private final String componentName;
      private final String componentType;
      private final boolean lazy;
      private transient Field field;
      private transient Class<?> componentClass;

      public InjectFieldMetadata(String fieldClassName, String fieldName, String componentType, String componentName,
                                 boolean lazy) {
         this.fieldClassName = fieldClassName;
         this.fieldName = fieldName;
         this.componentName = componentName;
         this.componentType = componentType;
         this.lazy = lazy;
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

      public String getDependencyName() {
         return componentName != null ? componentName : componentType;
      }

      public boolean isLazy() {
         return lazy;
      }

      @Override
      public String toString() {
         return "InjectFieldMetadata{" +
                "fieldName='" + fieldName + '\'' +
                ", componentType='" + componentType + '\'' +
                ", componentName='" + componentName + '\'' +
                ", lazy=" + lazy +
                ", fieldClassName='" + fieldClassName + '\'' +
                '}';
      }
   }
}
