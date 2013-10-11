package org.infinispan.factories.components;

import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.annotations.SurvivesRestarts;
import org.infinispan.factories.scopes.Scope;

import java.io.Serializable;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

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
   public static final InjectMetadata[] EMPTY_INJECT_METHODS = {};
   public static final PrioritizedMethodMetadata[] EMPTY_PRIORITIZED_METHODS = {};

   private String name;
   private transient Map<String, String> dependencies;
   private InjectMetadata[] injectMetadata;
   private PrioritizedMethodMetadata[] startMethods;
   private PrioritizedMethodMetadata[] stopMethods;
   private boolean globalScope = false;
   private boolean survivesRestarts = false;
   private transient Class<?> clazz;

   ComponentMetadata() {
      globalScope = false;
      survivesRestarts = true;
   }

   public ComponentMetadata(Class<?> component, List<Method> injectMethods, List<Method> startMethods, List<Method> stopMethods, boolean global, boolean survivesRestarts) {
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

      if (stopMethods != null && !stopMethods.isEmpty()) {
         this.stopMethods = new PrioritizedMethodMetadata[stopMethods.size()];
         int i=0;
         for (Method m : stopMethods) {
            Stop s = m.getAnnotation(Stop.class);
            this.stopMethods[i++] = new PrioritizedMethodMetadata(m.getName(), s.priority());
         }
      }

      if (injectMethods != null && !injectMethods.isEmpty()) {
         this.injectMetadata = new InjectMetadata[injectMethods.size()];
         this.dependencies = new HashMap<String, String>(injectMethods.size() * 2);
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

   public InjectMetadata[] getInjectMethods() {
      if (injectMetadata == null) return EMPTY_INJECT_METHODS;
      return injectMetadata;
   }

   public PrioritizedMethodMetadata[] getStartMethods() {
      if (startMethods == null) return EMPTY_PRIORITIZED_METHODS;
      return startMethods;
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
   }
}
