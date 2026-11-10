package org.infinispan.query.objectfilter.impl.syntax.update;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.query.functions.QueryFunction;
import org.infinispan.query.functions.QueryFunctionSignature;
import org.infinispan.query.functions.builtin.ConversionFunctions;
import org.infinispan.query.functions.builtin.DateFunctions;
import org.infinispan.query.functions.builtin.MathFunctions;
import org.infinispan.query.functions.builtin.RegexpFunctions;
import org.infinispan.query.functions.builtin.StringFunctions;

import com.google.errorprone.annotations.ThreadSafe;

/**
 * Registry of the named functions usable in statement value expressions.
 * <p>
 * Function names are case-insensitive. Each cache manager owns its own instance, created by
 * {@link #standard()} and registered as a global component, so user-defined functions added
 * to one cache manager are never visible to another one in the same JVM. Functions are
 * expected to be registered at startup, before queries start using them.
 *
 * @since 16.4
 */
@ThreadSafe
@Scope(Scopes.GLOBAL)
public final class QueryFunctionRegistry {

   // Immutable templates of the built-in functions, copied by standard(). Never exposed.
   private static final Map<String, QueryFunction> BUILT_IN_FUNCTIONS = createBuiltIns();
   private static final Map<String, Class<?>> BUILT_IN_RETURN_TYPES = declaredReturnTypesOf(BUILT_IN_FUNCTIONS);

   private final ConcurrentHashMap<String, QueryFunction> functions;
   private final ConcurrentHashMap<String, Class<?>> declaredReturnTypes;

   private QueryFunctionRegistry(Map<String, QueryFunction> functions, Map<String, Class<?>> declaredReturnTypes) {
      this.functions = new ConcurrentHashMap<>(functions);
      this.declaredReturnTypes = new ConcurrentHashMap<>(declaredReturnTypes);
   }

   /**
    * Creates a new registry pre-populated with the built-in functions. Every call returns an
    * independent instance, so adding or removing functions on it does not affect any other registry.
    */
   public static QueryFunctionRegistry standard() {
      return new QueryFunctionRegistry(BUILT_IN_FUNCTIONS, BUILT_IN_RETURN_TYPES);
   }

   /**
    * Adds a user-defined function to this registry.
    *
    * @param name     the function name (case-insensitive)
    * @param function the function implementation
    * @throws IllegalArgumentException if a function with the given name already exists
    */
   public void add(String name, QueryFunction function) {
      String key = normalize(name);
      if (functions.putIfAbsent(key, function) != null) {
         throw new IllegalArgumentException("Function already registered: " + name);
      }
      Class<?> returnType = declaredReturnTypeOf(function);
      if (returnType != null) {
         declaredReturnTypes.put(key, returnType);
      }
   }

   /**
    * Removes a function from this registry.
    *
    * @param name the function name (case-insensitive)
    * @return the previous function, or {@code null} if not present
    */
   public QueryFunction remove(String name) {
      String key = normalize(name);
      declaredReturnTypes.remove(key);
      return functions.remove(key);
   }

   public QueryFunction get(String name) {
      return functions.get(normalize(name));
   }

   public boolean contains(String name) {
      return functions.containsKey(normalize(name));
   }

   public Set<String> names() {
      return functions.keySet();
   }

   private static String normalize(String name) {
      return name.toLowerCase(Locale.ROOT);
   }

   /**
    * Returns the return type declared via {@link QueryFunctionSignature} on the
    * function's {@code evaluate} method, or {@code null} when unknown. Used for
    * parse-time type checking of function call results.
    */
   public Class<?> getDeclaredReturnType(String name) {
      return declaredReturnTypes.get(normalize(name));
   }

   /**
    * Reads the {@code returns()} value of the {@link QueryFunctionSignature}
    * annotation on the {@code evaluate} method of the given function, if any.
    */
   private static Class<?> declaredReturnTypeOf(QueryFunction function) {
      for (Method method : function.getClass().getMethods()) {
         if (method.getName().equals("evaluate") && method.isAnnotationPresent(QueryFunctionSignature.class)) {
            return method.getAnnotation(QueryFunctionSignature.class).returns();
         }
      }
      return null;
   }

   private static Map<String, Class<?>> declaredReturnTypesOf(Map<String, QueryFunction> functions) {
      Map<String, Class<?>> types = new HashMap<>();
      functions.forEach((name, fn) -> {
         Class<?> returnType = declaredReturnTypeOf(fn);
         if (returnType != null) {
            types.put(name, returnType);
         }
      });
      return Map.copyOf(types);
   }

   private static Map<String, QueryFunction> createBuiltIns() {
      Map<String, QueryFunction> f = new HashMap<>();

      // math
      f.put("abs", new MathFunctions.Abs());
      f.put("round", new MathFunctions.Round());
      f.put("floor", new MathFunctions.Floor());
      f.put("ceil", new MathFunctions.Ceil());
      f.put("min", new MathFunctions.Min());
      f.put("max", new MathFunctions.Max());
      f.put("power", new MathFunctions.Power());
      f.put("sqrt", new MathFunctions.Sqrt());

      // string
      f.put("concat", new StringFunctions.Concat());
      f.put("upper", new StringFunctions.Upper());
      f.put("lower", new StringFunctions.Lower());
      f.put("trim", new StringFunctions.Trim());
      f.put("length", new StringFunctions.Length());
      f.put("substring", new StringFunctions.Substring());
      f.put("replace", new StringFunctions.Replace());

      // regexp
      f.put("regexp_like", new RegexpFunctions.RegexpLike());
      f.put("regexp_replace", new RegexpFunctions.RegexpReplace());
      f.put("regexp_substr", new RegexpFunctions.RegexpSubstr());
      f.put("regexp_instr", new RegexpFunctions.RegexpInstr());

      // date
      f.put("adddays", new DateFunctions.AddDays());
      f.put("addhours", new DateFunctions.AddHours());
      f.put("addminutes", new DateFunctions.AddMinutes());
      f.put("addmonths", new DateFunctions.AddMonths());
      f.put("addyears", new DateFunctions.AddYears());
      f.put("dateadd", new DateFunctions.DateAdd());

      // conversion
      f.put("tolong", new ConversionFunctions.ToLong());
      f.put("todouble", new ConversionFunctions.ToDouble());
      f.put("tostring", new ConversionFunctions.ToString());

      return Map.copyOf(f);
   }
}
