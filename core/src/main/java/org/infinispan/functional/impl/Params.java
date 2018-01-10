package org.infinispan.functional.impl;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.List;

import org.infinispan.functional.Param;
import org.infinispan.functional.Param.ExecutionMode;
import org.infinispan.functional.Param.LockingMode;
import org.infinispan.functional.Param.PersistenceMode;
import org.infinispan.functional.Param.StatisticsMode;
import org.infinispan.commons.util.Experimental;
import org.infinispan.context.impl.FlagBitSets;

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
      PersistenceMode.defaultValue(), LockingMode.defaultValue(), ExecutionMode.defaultValue(), StatisticsMode.defaultValue()
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

   public Params addAll(Params ps) {
      if (ps == DEFAULT_INSTANCE) {
         return this;
      }
      Param<?>[] paramsAll = Arrays.copyOf(params, params.length);
      for (int i = 0; i < this.params.length; ++i) {
         if (!ps.params[i].equals(DEFAULTS[i])) {
            paramsAll[i] = ps.params[i];
         }
      }
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

   /**
    * Bridging method between flags and params, provided for efficient checks.
    */
   public long toFlagsBitSet() {
      PersistenceMode persistenceMode = (PersistenceMode) params[PersistenceMode.ID].get();
      LockingMode lockingMode = (LockingMode) params[LockingMode.ID].get();
      ExecutionMode executionMode = (ExecutionMode) params[ExecutionMode.ID].get();
      StatisticsMode statisticsMode = (StatisticsMode) params[StatisticsMode.ID].get();
      long flagsBitSet = 0;
      switch (persistenceMode) {
         case LOAD_PERSIST:
            break;
         case SKIP_PERSIST:
            flagsBitSet |= FlagBitSets.SKIP_CACHE_STORE;
            break;
         case SKIP_LOAD:
            flagsBitSet |= FlagBitSets.SKIP_CACHE_LOAD;
            break;
         case SKIP:
            flagsBitSet |= FlagBitSets.SKIP_CACHE_LOAD | FlagBitSets.SKIP_CACHE_STORE;
            break;
      }
      if (lockingMode == LockingMode.SKIP) flagsBitSet |= FlagBitSets.SKIP_LOCKING;
      if (executionMode == ExecutionMode.LOCAL) flagsBitSet |= FlagBitSets.CACHE_MODE_LOCAL;
      else if (executionMode == ExecutionMode.LOCAL_SITE) flagsBitSet |= FlagBitSets.SKIP_XSITE_BACKUP;
      if (statisticsMode == StatisticsMode.SKIP) flagsBitSet |= FlagBitSets.SKIP_STATISTICS;
      return flagsBitSet;
   }

   public static Params fromFlagsBitSet(long flagsBitSet) {
      Params params = create();
      if ((flagsBitSet & (FlagBitSets.SKIP_CACHE_LOAD | FlagBitSets.SKIP_CACHE_STORE)) != 0) {
         params = params.addAll(PersistenceMode.SKIP);
      } else if ((flagsBitSet & FlagBitSets.SKIP_CACHE_STORE) != 0) {
         params = params.addAll(PersistenceMode.SKIP_PERSIST);
      } else if ((flagsBitSet & FlagBitSets.SKIP_CACHE_LOAD) != 0) {
         params = params.addAll(PersistenceMode.SKIP_LOAD);
      }
      if ((flagsBitSet & FlagBitSets.SKIP_LOCKING) != 0) {
         params = params.addAll(LockingMode.SKIP);
      }
      if ((flagsBitSet & FlagBitSets.CACHE_MODE_LOCAL) != 0) {
         params = params.addAll(ExecutionMode.LOCAL);
      } else if ((flagsBitSet & FlagBitSets.SKIP_XSITE_BACKUP) != 0) {
         params = params.addAll(ExecutionMode.LOCAL_SITE);
      }
      if ((flagsBitSet & FlagBitSets.SKIP_STATISTICS) != 0) {
         params = params.addAll(StatisticsMode.SKIP);
      }
      return params;
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

   static {
      // make sure that bit-set marshalling will work
      if (PersistenceMode.values().length > 4) throw new IllegalStateException();
      if (LockingMode.values().length > 2) throw new IllegalStateException();
      if (ExecutionMode.values().length > 4) throw new IllegalStateException();
      if (StatisticsMode.values().length > 2) throw new IllegalStateException();
   }

   public static void writeObject(ObjectOutput output, Params params) throws IOException {
      PersistenceMode persistenceMode = (PersistenceMode) params.get(PersistenceMode.ID).get();
      LockingMode lockingMode = (LockingMode) params.get(LockingMode.ID).get();
      ExecutionMode executionMode = (ExecutionMode) params.get(ExecutionMode.ID).get();
      StatisticsMode statisticsMode = (StatisticsMode) params.get(StatisticsMode.ID).get();
      int paramBits = persistenceMode.ordinal()
            | (lockingMode.ordinal() << 2)
            | (executionMode.ordinal() << 3)
            | (statisticsMode.ordinal() << 5);
      output.writeByte(paramBits);
   }

   public static Params readObject(ObjectInput input) throws IOException, ClassNotFoundException {
      int paramBits = input.readByte();
      PersistenceMode persistenceMode = PersistenceMode.valueOf(paramBits & 3);
      LockingMode lockingMode = LockingMode.valueOf((paramBits >>> 2) & 1);
      ExecutionMode executionMode = ExecutionMode.valueOf((paramBits >>> 3) & 3);
      StatisticsMode statisticsMode = StatisticsMode.valueOf((paramBits >>> 5) & 1);
      if (persistenceMode == PersistenceMode.defaultValue()
            && lockingMode == LockingMode.defaultValue()
            && executionMode == ExecutionMode.defaultValue()
            && statisticsMode == StatisticsMode.defaultValue()) {
         return DEFAULT_INSTANCE;
      } else {
         Param[] params = Arrays.copyOf(DEFAULTS, DEFAULTS.length);
         params[PersistenceMode.ID] = persistenceMode;
         params[LockingMode.ID] = lockingMode;
         params[ExecutionMode.ID] = executionMode;
         params[StatisticsMode.ID] = statisticsMode;
         return new Params(params);
      }
   }
}
