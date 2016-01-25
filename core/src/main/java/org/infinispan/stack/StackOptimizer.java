package org.infinispan.stack;

import javassist.CannotCompileException;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.NotFoundException;
import javassist.bytecode.BadBytecode;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

/**
 * Holds modified classes, arguments and all the generic configuration, and links
 * optimized interceptors.
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class StackOptimizer<V, C, N> {
   private final ClassPool classPool = ClassPool.getDefault();
   private final static ConcurrentHashMap<OptimizationKey, CompletableFuture<GeneratedClasses>> optimizedClasses = new ConcurrentHashMap<>();
   protected CtClass visitorClass;
   protected CtClass interceptorClass;
   protected CtClass commandClass;
   protected String nextMethodName;
   protected String nextMethodSignature;
   protected Class<? extends N> nextFieldClass;
   protected String nextFieldName;
   protected Field nextField;
   protected String acceptMethodName;
   protected String acceptMethodSignature;
   protected int[] acceptArgs;
   protected String performMethodName;
   protected String performMethodSignature;
   protected int[] performArgs;
   protected final Map<Object, Object> replacedInterceptors = new HashMap<>();

   public IllegalArgumentException rewrap(Exception e) {
      return new IllegalArgumentException("Cannot optimize stack", e);
   }

   /**
    * Receives created interceptor stack and returns initialized optimized stack.
    *
    * @param visitor First interceptor in the stack.
    * @return
    */
   public V optimize(V visitor) {
      ArrayList<V> interceptors = new ArrayList<>();
      ArrayList<Class<?>> classes = new ArrayList<>();
      try {
         while (visitor != null) {
            interceptors.add(visitor);
            classes.add(visitor.getClass());
            visitor = (V) nextField.get(visitor);
         }

         CtClass nextClass = null;
         CtClass nextVirtualDelegator = null;
         Object nextReplacement = null;
         Class<?> nextJavaClass = null, nextJavaVirtualDelegator = null;
         for (int i = interceptors.size() - 1; i >= 0; --i) {
            OptimizationKey key = new OptimizationKey(this, classes.subList(i, classes.size()));
            CompletableFuture<GeneratedClasses> newPromise = new CompletableFuture<>();
            CompletableFuture<GeneratedClasses> existingPromise = optimizedClasses.putIfAbsent(key, newPromise);
            GeneratedClasses generatedClasses;
            Class<?> javaClass = classes.get(i);
            if (existingPromise == null) {
               String className = javaClass.getName();
               CtClass oldClass = classPool.get(className);

               InterceptorGenerator interceptorGenerator = new InterceptorGenerator(this, oldClass, nextClass, nextVirtualDelegator);
               interceptorGenerator.run();
               interceptorGenerator.compile();
               DelegatorGenerator delegatorGenerator = new DelegatorGenerator(this, interceptorGenerator.getNewInterceptor());
               delegatorGenerator.generate();
               generatedClasses = new GeneratedClasses(
                     interceptorGenerator.getNewInterceptor(), delegatorGenerator.getNewDelegator(),
                     interceptorGenerator.getNewJavaInterceptor(), delegatorGenerator.getNewJavaDelegator());
               newPromise.complete(generatedClasses);
            } else {
               generatedClasses = existingPromise.get();
            }

            Class<?> newJavaClass = generatedClasses.javaInterceptor;
            Object replacement;
            if (nextJavaClass != null) {
               Constructor constructor = newJavaClass.getDeclaredConstructor(new Class[] {javaClass, nextJavaClass});
               replacement = constructor.newInstance(interceptors.get(i), nextReplacement);
            } else {
               Constructor constructor = newJavaClass.getDeclaredConstructor(new Class[] { javaClass });
               replacement = constructor.newInstance(interceptors.get(i));
            }
            replacedInterceptors.put(interceptors.get(i), replacement);

            nextClass = generatedClasses.interceptor;
            nextJavaClass = newJavaClass;
            nextVirtualDelegator = generatedClasses.delegator;
            nextJavaVirtualDelegator = generatedClasses.javaDelegator;
            nextReplacement = replacement;
         }

         return (V) nextJavaVirtualDelegator.getDeclaredConstructor(nextJavaClass).newInstance(nextReplacement);
      } catch (IOException e) {
         throw rewrap(e);
      } catch (CannotCompileException e) {
         throw rewrap(e);
      } catch (BadBytecode e) {
         throw rewrap(e);
      } catch (IllegalAccessException e) {
         throw rewrap(e);
      } catch (NotFoundException e) {
         throw rewrap(e);
      } catch (NoSuchMethodException e) {
         throw rewrap(e);
      } catch (InstantiationException e) {
         throw rewrap(e);
      } catch (InvocationTargetException e) {
         throw rewrap(e);
      } catch (InterruptedException e) {
         throw rewrap(e);
      } catch (ExecutionException e) {
         throw rewrap(e);
      }
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      StackOptimizer<?, ?, ?> that = (StackOptimizer<?, ?, ?>) o;

      if (!visitorClass.equals(that.visitorClass)) return false;
      if (!commandClass.equals(that.commandClass)) return false;
      if (!nextMethodName.equals(that.nextMethodName)) return false;
      if (!nextMethodSignature.equals(that.nextMethodSignature)) return false;
      if (!nextFieldClass.equals(that.nextFieldClass)) return false;
      if (!nextFieldName.equals(that.nextFieldName)) return false;
      if (!nextField.equals(that.nextField)) return false;
      if (!acceptMethodName.equals(that.acceptMethodName)) return false;
      if (!acceptMethodSignature.equals(that.acceptMethodSignature)) return false;
      if (!Arrays.equals(acceptArgs, that.acceptArgs)) return false;
      if (!performMethodName.equals(that.performMethodName)) return false;
      if (!performMethodSignature.equals(that.performMethodSignature)) return false;
      return Arrays.equals(performArgs, that.performArgs);

   }

   @Override
   public int hashCode() {
      int result = visitorClass.hashCode();
      result = 31 * result + commandClass.hashCode();
      result = 31 * result + nextMethodName.hashCode();
      result = 31 * result + nextMethodSignature.hashCode();
      result = 31 * result + nextFieldClass.hashCode();
      result = 31 * result + nextFieldName.hashCode();
      result = 31 * result + nextField.hashCode();
      result = 31 * result + acceptMethodName.hashCode();
      result = 31 * result + acceptMethodSignature.hashCode();
      result = 31 * result + Arrays.hashCode(acceptArgs);
      result = 31 * result + performMethodName.hashCode();
      result = 31 * result + performMethodSignature.hashCode();
      result = 31 * result + Arrays.hashCode(performArgs);
      return result;
   }

   public Map<Object, Object> getReplacedInterceptors() {
      return replacedInterceptors;
   }

   public StackOptimizer<V, C, N> visitor(Class<V> visitorClass) {
      try {
         this.visitorClass = classPool.get(visitorClass.getName());
         return this;
      } catch (NotFoundException e) {
         throw rewrap(e);
      }
   }

   public StackOptimizer<V, C, N> interceptor(Class<? extends V> interceptorClass) {
      try {
         this.interceptorClass = classPool.getCtClass(interceptorClass.getName());
         return this;
      } catch (NotFoundException e) {
         throw rewrap(e);
      }
   }

   public StackOptimizer<V, C, N> command(Class<C> commandClass) {
      try {
         this.commandClass = classPool.get(commandClass.getName());
         return this;
      } catch (NotFoundException e) {
         throw rewrap(e);
      }
   }

   public StackOptimizer<V, C, N> nextMethod(String nextMethodName, String nextMethodSignature) {
      this.nextMethodName = nextMethodName;
      this.nextMethodSignature = nextMethodSignature;
      return this;
   }

   public StackOptimizer<V, C, N> nextField(Class<? extends N> nextFieldClass, String nextFieldName) {
      this.nextFieldClass = nextFieldClass;
      this.nextFieldName = nextFieldName;
      try {
         this.nextField = nextFieldClass.getDeclaredField(nextFieldName);
      } catch (NoSuchFieldException e) {
         throw rewrap(e);
      }
      nextField.setAccessible(true);
      return this;
   }

   public StackOptimizer<V, C, N> acceptMethod(String acceptMethodName, String acceptMethodSignature, int[] acceptArgs) {
      this.acceptMethodName = acceptMethodName;
      this.acceptMethodSignature = acceptMethodSignature;
      this.acceptArgs = acceptArgs;
      return this;
   }

   public StackOptimizer<V, C, N> performMethod(String performMethodName, String performMethodSignature, int[] performArgs) {
      this.performMethodName = performMethodName;
      this.performMethodSignature = performMethodSignature;
      this.performArgs = performArgs;
      return this;
   }

   private static class GeneratedClasses {
      final CtClass interceptor;
      final CtClass delegator;
      final Class javaInterceptor;
      final Class javaDelegator;

      public GeneratedClasses(CtClass interceptor, CtClass delegator, Class javaInterceptor, Class javaDelegator) {
         this.interceptor = interceptor;
         this.delegator = delegator;
         this.javaInterceptor = javaInterceptor;
         this.javaDelegator = javaDelegator;
      }
   }

   private static class OptimizationKey {
      private final StackOptimizer stackOptimizer;
      private final List<Class<?>> stackSuffix;

      private OptimizationKey(StackOptimizer stackOptimizer, List<Class<?>> stackSuffix) {
         this.stackOptimizer = stackOptimizer;
         this.stackSuffix = stackSuffix;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;

         OptimizationKey that = (OptimizationKey) o;

         if (!stackOptimizer.equals(that.stackOptimizer)) return false;
         return stackSuffix.equals(that.stackSuffix);

      }

      @Override
      public int hashCode() {
         int result = stackOptimizer.hashCode();
         result = 31 * result + stackSuffix.hashCode();
         return result;
      }
   }
}
