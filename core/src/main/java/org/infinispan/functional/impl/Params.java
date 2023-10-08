package org.infinispan.functional.impl;

import java.util.Arrays;
import java.util.List;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.commons.util.Experimental;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.functional.Param;
import org.infinispan.functional.Param.ExecutionMode;
import org.infinispan.functional.Param.LockingMode;
import org.infinispan.functional.Param.PersistenceMode;
import org.infinispan.functional.Param.ReplicationMode;
import org.infinispan.functional.Param.StatisticsMode;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

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
@ProtoTypeId(ProtoStreamTypeIds.FUNCTIONAL_PARAMS)
public final class Params {

   private static final Param<?>[] DEFAULTS = new Param<?>[]{
         PersistenceMode.defaultValue(), LockingMode.defaultValue(), ExecutionMode.defaultValue(),
         StatisticsMode.defaultValue(), ReplicationMode.defaultValue()
   };
   // TODO: as Params are immutable and there's only limited number of them,
   // there could be a table with all the possible combinations and we
   // wouldn't have to allocate at all
   private static final Params DEFAULT_INSTANCE = new Params(DEFAULTS);

   final Param<?>[] params;

   private Params(Param<?>[] params) {
      this.params = params;
   }

   @ProtoFactory
   static Params protoFactory (byte bits) {
      PersistenceMode persistenceMode = PersistenceMode.valueOf(bits & 3);
      LockingMode lockingMode = LockingMode.valueOf((bits >>> 2) & 3);
      ExecutionMode executionMode = ExecutionMode.valueOf((bits >>> 4) & 3);
      StatisticsMode statisticsMode = StatisticsMode.valueOf((bits >>> 6) & 1);
      ReplicationMode replicationMode = ReplicationMode.valueOf((bits >>> 7) & 1);
      if (persistenceMode == PersistenceMode.defaultValue()
            && lockingMode == LockingMode.defaultValue()
            && executionMode == ExecutionMode.defaultValue()
            && statisticsMode == StatisticsMode.defaultValue()
            && replicationMode == ReplicationMode.defaultValue()) {
         return DEFAULT_INSTANCE;
      } else {
         Param[] params = Arrays.copyOf(DEFAULTS, DEFAULTS.length);
         params[PersistenceMode.ID] = persistenceMode;
         params[LockingMode.ID] = lockingMode;
         params[ExecutionMode.ID] = executionMode;
         params[StatisticsMode.ID] = statisticsMode;
         params[ReplicationMode.ID] = replicationMode;
         return new Params(params);
      }
   }

   // TODO should we hardcode the bits for the DEFAULT_INSTANCE?
   @ProtoField(1)
   byte getBits() {
      PersistenceMode persistenceMode = (PersistenceMode) get(PersistenceMode.ID).get();
      LockingMode lockingMode = (LockingMode) get(LockingMode.ID).get();
      ExecutionMode executionMode = (ExecutionMode) get(ExecutionMode.ID).get();
      StatisticsMode statisticsMode = (StatisticsMode) get(StatisticsMode.ID).get();
      ReplicationMode replicationMode = (ReplicationMode) get(ReplicationMode.ID).get();
      return (byte) (persistenceMode.ordinal()
            | (lockingMode.ordinal() << 2)
            | (executionMode.ordinal() << 4)
            | (statisticsMode.ordinal() << 6)
            | (replicationMode.ordinal() << 7));
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

   public Params add(Param<?> p) {
      Param<?>[] paramsAll = Arrays.copyOf(params, params.length);
      paramsAll[p.id()] = p;
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
      ReplicationMode replicationMode = (ReplicationMode) params[ReplicationMode.ID].get();
      long flagsBitSet = 0;
      switch (persistenceMode) {
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
      switch (lockingMode) {
         case SKIP:
            flagsBitSet |= FlagBitSets.SKIP_LOCKING;
            break;
         case TRY_LOCK:
            flagsBitSet |= FlagBitSets.ZERO_LOCK_ACQUISITION_TIMEOUT | FlagBitSets.FAIL_SILENTLY;
            break;
      }
      switch (executionMode) {
         case LOCAL:
            flagsBitSet |= FlagBitSets.CACHE_MODE_LOCAL;
            break;
         case LOCAL_SITE:
            flagsBitSet |= FlagBitSets.SKIP_XSITE_BACKUP;
            break;
      }
      if (statisticsMode == StatisticsMode.SKIP) {
         flagsBitSet |= FlagBitSets.SKIP_STATISTICS;
      }
      switch (replicationMode) {
         case SYNC:
            flagsBitSet |= FlagBitSets.FORCE_SYNCHRONOUS;
            break;
         case ASYNC:
            flagsBitSet |= FlagBitSets.FORCE_ASYNCHRONOUS;
            break;
      }
      return flagsBitSet;
   }

   public static Params fromFlagsBitSet(long flagsBitSet) {
      if (flagsBitSet == 0) {
         return DEFAULT_INSTANCE;
      }
      Param<?>[] paramsAll = Arrays.copyOf(DEFAULTS, DEFAULTS.length);
      if ((flagsBitSet & (FlagBitSets.SKIP_CACHE_LOAD | FlagBitSets.SKIP_CACHE_STORE)) != 0) {
         paramsAll[PersistenceMode.ID] = PersistenceMode.SKIP;
      } else if ((flagsBitSet & FlagBitSets.SKIP_CACHE_STORE) != 0) {
         paramsAll[PersistenceMode.ID] = PersistenceMode.SKIP_PERSIST;
      } else if ((flagsBitSet & FlagBitSets.SKIP_CACHE_LOAD) != 0) {
         paramsAll[PersistenceMode.ID] = PersistenceMode.SKIP_LOAD;
      }
      if ((flagsBitSet & FlagBitSets.SKIP_LOCKING) != 0) {
         paramsAll[LockingMode.ID] = LockingMode.SKIP;
      } else if ((flagsBitSet & FlagBitSets.ZERO_LOCK_ACQUISITION_TIMEOUT) != 0) {
         paramsAll[LockingMode.ID] = LockingMode.TRY_LOCK;
      }
      if ((flagsBitSet & FlagBitSets.CACHE_MODE_LOCAL) != 0) {
         paramsAll[ExecutionMode.ID] = ExecutionMode.LOCAL;
      } else if ((flagsBitSet & FlagBitSets.SKIP_XSITE_BACKUP) != 0) {
         paramsAll[ExecutionMode.ID] = ExecutionMode.LOCAL_SITE;
      }
      if ((flagsBitSet & FlagBitSets.SKIP_STATISTICS) != 0) {
         paramsAll[StatisticsMode.ID] = StatisticsMode.SKIP;
      }
      if ((flagsBitSet & FlagBitSets.FORCE_ASYNCHRONOUS) != 0) {
         paramsAll[ReplicationMode.ID] = ReplicationMode.ASYNC;
      } else if ((flagsBitSet & FlagBitSets.FORCE_SYNCHRONOUS) != 0) {
         paramsAll[ReplicationMode.ID] = ReplicationMode.SYNC;
      }
      return new Params(paramsAll);
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
      if (LockingMode.values().length > 4) throw new IllegalStateException();
      if (ExecutionMode.values().length > 4) throw new IllegalStateException();
      if (StatisticsMode.values().length > 2) throw new IllegalStateException();
      if (ReplicationMode.values().length > 2) throw new IllegalStateException();
   }
}
