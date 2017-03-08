package org.infinispan.functional.impl;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.List;

import org.infinispan.commons.api.functional.Param;
import org.infinispan.commons.api.functional.Param.PersistenceMode;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.commons.util.Experimental;

/**
 * Internal class that encapsulates collection of parameters used to tweak
 * functional map operations.
 *
 * <p>Internally, parameters are stored in an array which is indexed by
 * a parameter's {@link Param#id()}
 *
 * <p>All parameters have default values which are stored in a static
 * array field in {@link Params} class, which are used to as base collection
 * when adding or overriding parameters.
 *
 * @since 8.0
 */
@Experimental
public final class Params {

   private static final Param<?>[] DEFAULTS = new Param<?>[]{
      PersistenceMode.defaultValue(),
   };
   // TODO: as Params are immutable and there's only limited number of them,
   // there could be a table with all the possible combinations and we
   // wouldn't have to allocate at all
   private static final Params DEFAULT_INSTANCE = new Params(DEFAULTS);

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
      // TODO: Provide a more type safe API. E.g. make index a pojo typed on T which is aligned with the Param<T>
      return (Param<T>) params[index];
   }

   @Override
   public String toString() {
      return "Params=" + Arrays.toString(params);
   }

   public static Params create() {
      return DEFAULT_INSTANCE;
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

   public static void writeObject(ObjectOutput output, Params params) throws IOException {
      // There's no point in sending FutureMode over wire
      MarshallUtil.marshallEnum((PersistenceMode) params.get(PersistenceMode.ID).get(), output);
   }

   public static Params readObject(ObjectInput input) throws IOException, ClassNotFoundException {
      PersistenceMode persistenceMode = MarshallUtil.unmarshallEnum(input, PersistenceMode::valueOf);
      if (persistenceMode == PersistenceMode.defaultValue()) {
         return DEFAULT_INSTANCE;
      } else {
         Param[] params = Arrays.copyOf(DEFAULTS, DEFAULTS.length);
         params[PersistenceMode.ID] = persistenceMode;
         return new Params(params);
      }
   }
}
