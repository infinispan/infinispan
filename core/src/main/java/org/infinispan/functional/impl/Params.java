package org.infinispan.functional.impl;

import org.infinispan.commons.api.functional.Param;

import java.util.Arrays;
import java.util.List;

/**
 * Internal class that encapsulates collection of parameters used to tweak
 * functional map operations.
 *
 * DESIGN RATIONALES:
 * <ul>
 *    <il>Internally, parameters are stored in an array which is indexed by a
 *    parameter's {@link Param#id()}
 *    </il>
 *    <il>All parameters have default values which are stored in a static
 *    array field in {@link Params} class, which are used to as base collection
 *    when adding or overriding parameters.
 *    </il>
 * </ul>
 *
 * @since 8.0
 */
final class Params {

   private static final Param<?>[] DEFAULTS = new Param<?>[]{
      Param.WaitMode.defaultValue(),
   };

   final Param<?>[] params;

   private Params(Param<?>[] params) {
      this.params = params;
   }

   /**
    * Checks whether all the parameters passed in are already present in the
    * current parameters. This method can be used to optimise the decision on
    * whether the parameters collection needs updating at all.
    */
   public boolean containsAll(Param<?>... ps) {
      List<Param<?>> paramsToCheck = Arrays.asList(ps);
      List<Param<?>> paramsCurrent = Arrays.asList(params);
      return paramsCurrent.containsAll(paramsToCheck);
   }

   /**
    * Adds all parameters and returns a new parameter collection.
    */
   public Params addAll(Param<?>... ps) {
      List<Param<?>> paramsToAdd = Arrays.asList(ps);
      Param<?>[] paramsAll = Arrays.copyOf(params, params.length);
      paramsToAdd.forEach(p -> paramsAll[p.id()] = p);
      return new Params(paramsAll);
   }

   /**
    * Retrieve a param given its identifier. Callers are expected to know the
    * exact type of parameter that will be returned. Such assumption is
    * possible because as indicated in {@link Param} implementations will
    * only come from Infinispan itself.
    */
   @SuppressWarnings("unchecked")
   public <T> Param<T> get(int index) {
      return (Param<T>) params[index];
   }

   @Override
   public String toString() {
      return "Params=" + Arrays.toString(params);
   }

   public static Params create() {
      return new Params(DEFAULTS);
   }

   public static Params from(Param<?>... ps) {
      List<Param<?>> paramsToAdd = Arrays.asList(ps);
      List<Param<?>> paramsDefaults = Arrays.asList(DEFAULTS);
      if (paramsDefaults.containsAll(paramsToAdd))
         return create(); // All parameters are defaults, don't do more work

      Param<?>[] paramsAll = Arrays.copyOf(DEFAULTS, DEFAULTS.length);
      paramsToAdd.forEach(p -> paramsAll[p.id()] = p);
      return new Params(paramsAll);
   }

}
