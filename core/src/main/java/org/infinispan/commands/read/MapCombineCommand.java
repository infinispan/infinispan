package org.infinispan.commands.read;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.infinispan.commands.CancellableCommand;
import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.distexec.mapreduce.MapReduceManager;
import org.infinispan.distexec.mapreduce.Mapper;
import org.infinispan.distexec.mapreduce.Reducer;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * MapCombineCommand is a container to migrate {@link Mapper} and {@link Reducer} which is a
 * combiner to a remote Infinispan node where it will get executed and return the result to an
 * invoking/master node.
 * 
 * @author Vladimir Blagojevic
 * @since 5.0
 */
public class MapCombineCommand<KIn, VIn, KOut, VOut> extends BaseRpcCommand implements CancellableCommand {
   public static final int COMMAND_ID = 30;
   private static final Log log = LogFactory.getLog(MapCombineCommand.class);
   private Set<KIn> keys = new HashSet<KIn>();
   private Mapper<KIn, VIn, KOut, VOut> mapper;
   private Reducer<KOut, VOut> combiner;  
   private String taskId;
   private boolean reducePhaseDistributed;
   private boolean emitCompositeIntermediateKeys;
   private MapReduceManager mrManager;
   private UUID uuid;

   public MapCombineCommand() {
      super(null); // For command id uniqueness test
   }

   public MapCombineCommand(String cacheName) {
      super(cacheName);
   }

   public MapCombineCommand(String taskId, Mapper<KIn, VIn, KOut, VOut> mapper,
            Reducer<KOut, VOut> combiner, String cacheName, Collection<KIn> inputKeys) {
      super(cacheName);
      this.taskId = taskId;
      if (inputKeys != null && !inputKeys.isEmpty()) {
         keys.addAll(inputKeys);
      }
      this.mapper = mapper;
      this.combiner = combiner;
      this.uuid = UUID.randomUUID();
   }

   public void init(MapReduceManager mrManager) {
      this.mrManager = mrManager;
   }

   /**
    * Performs invocation of mapping phase and local combine phase on assigned Infinispan node
    * 
    * @param context
    *           invocation context
    * @return Map of intermediate key value pairs
    */
   @Override
   public Object perform(InvocationContext context) throws Throwable {
      if (isReducePhaseDistributed())
         return mrManager.mapAndCombineForDistributedReduction(this);
      else
         return mrManager.mapAndCombineForLocalReduction(this);
   }

   public boolean isEmitCompositeIntermediateKeys() {
      return emitCompositeIntermediateKeys;
   }

   public void setEmitCompositeIntermediateKeys(boolean emitCompositeIntermediateKeys) {
      this.emitCompositeIntermediateKeys = emitCompositeIntermediateKeys;
   }

   public boolean isReducePhaseDistributed() {
      return reducePhaseDistributed;
   }

   public void setReducePhaseDistributed(boolean reducePhaseDistributed) {
      this.reducePhaseDistributed = reducePhaseDistributed;
   }

   public Set<KIn> getKeys() {
      return keys;
   }

   public Mapper<KIn, VIn, KOut, VOut> getMapper() {
      return mapper;
   }

   public Reducer<KOut, VOut> getCombiner() {
      return combiner;
   }
   
   public boolean hasCombiner(){
      return getCombiner() != null;
   }

   public String getTaskId() {
      return taskId;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public UUID getUUID() {      
      return uuid;
   }

   @Override
   public Object[] getParameters() {
      return new Object[] { taskId, keys, mapper, combiner, reducePhaseDistributed,
               emitCompositeIntermediateKeys, uuid };
   }

   @SuppressWarnings("unchecked")
   @Override
   public void setParameters(int commandId, Object[] args) {
      if (commandId != COMMAND_ID)
         throw new IllegalStateException("Invalid method id");
      int i = 0;
      taskId = (String) args[i++];
      keys = (Set<KIn>) args[i++];
      mapper = (Mapper<KIn, VIn, KOut, VOut>) args[i++];
      combiner = (Reducer<KOut,VOut>) args[i++];
      reducePhaseDistributed = (Boolean) args[i++];
      emitCompositeIntermediateKeys = (Boolean) args[i++];
      uuid = (UUID) args[i++];
   }

   @Override
   public boolean isReturnValueExpected() {
      return true;
   }

   @Override
   public boolean canBlock() {
      return true;
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((taskId == null) ? 0 : taskId.hashCode());
      return result;
   }

   @SuppressWarnings("rawtypes")
   @Override
   public boolean equals(Object obj) {
      if (this == obj) {
         return true;
      }
      if (obj == null) {
         return false;
      }
      if (!(obj instanceof MapCombineCommand)) {
         return false;
      }
      MapCombineCommand other = (MapCombineCommand) obj;
      if (taskId == null) {
         if (other.taskId != null) {
            return false;
         }
      } else if (!taskId.equals(other.taskId)) {
         return false;
      }
      return true;
   }

   @Override
   public String toString() {
      return "MapCombineCommand [keys=" + keys + ", taskId=" + taskId + "]";
   }
}