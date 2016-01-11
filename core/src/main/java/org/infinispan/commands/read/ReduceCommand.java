package org.infinispan.commands.read;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.infinispan.commands.CancellableCommand;
import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.context.InvocationContext;
import org.infinispan.distexec.mapreduce.MapReduceManager;
import org.infinispan.distexec.mapreduce.Reducer;

/**
 * ReduceCommand is a container to migrate {@link Reducer} to a remote Infinispan node where it will
 * get executed and return the result to an invoking/master node.
 * 
 * @author Vladimir Blagojevic
 * @since 5.2
 */
public class ReduceCommand<KOut, VOut> extends BaseRpcCommand implements CancellableCommand {

   public static final int COMMAND_ID = 31;
   private Set<KOut> keys = new HashSet<KOut>();
   private Reducer<KOut, VOut> reducer;
   private String taskId;
   private boolean useIntermediateSharedCache;
   private MapReduceManager mrManager;
   private UUID uuid;
   private String resultCacheName;

   private ReduceCommand() {
      super(null); // For command id uniqueness test
   }

   public ReduceCommand(String cacheName) {
      super(cacheName);
   }

   public ReduceCommand(String taskId, Reducer<KOut, VOut> reducer, String cacheName,
            Collection<KOut> inputKeys) {
      super(cacheName);
      this.taskId = taskId;
      if (inputKeys != null && !inputKeys.isEmpty()) {
         keys.addAll(inputKeys);
      }
      this.reducer = reducer;
      this.uuid = UUID.randomUUID();
   }

   public void init(MapReduceManager mrManager) {
      this.mrManager = mrManager;
   }

   /**
    * Performs invocation of reduce phase on assigned Infinispan node
    * 
    * @param context
    *           invocation context
    * @return Map of intermediate key value pairs
    */
   @Override
   public Object perform(InvocationContext context) throws Throwable {
      if (emitsIntoResultingCache()){
         mrManager.reduce(this, getResultCacheName());
         return Collections.emptyMap();
      } else {
         return mrManager.reduce(this);
      }
   }

   public boolean isUseIntermediateSharedCache() {
      return useIntermediateSharedCache;
   }

   public void setUseIntermediateSharedCache(boolean useIntermediateSharedCache) {
      this.useIntermediateSharedCache = useIntermediateSharedCache;
   }

   public boolean emitsIntoResultingCache(){
      return resultCacheName != null && !resultCacheName.isEmpty();
   }

   public String getResultCacheName() {
      return resultCacheName;
   }

   public void setResultCacheName(String resultCacheName) {
      this.resultCacheName = resultCacheName;
   }

   public Set<KOut> getKeys() {
      return keys;
   }

   public Reducer<KOut, VOut> getReducer() {
      return reducer;
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
   public void writeTo(ObjectOutput output) throws IOException {
      output.writeUTF(taskId);
      MarshallUtil.marshallCollection(keys, output);
      output.writeObject(reducer);
      output.writeBoolean(useIntermediateSharedCache);
      MarshallUtil.marshallUUID(uuid, output, false);
      MarshallUtil.marshallString(resultCacheName, output);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      taskId = input.readUTF();
      keys = MarshallUtil.unmarshallCollectionUnbounded(input, HashSet::new);
      reducer = (Reducer<KOut, VOut>) input.readObject();
      useIntermediateSharedCache = input.readBoolean();
      uuid = MarshallUtil.unmarshallUUID(input, false);
      resultCacheName = MarshallUtil.unmarshallString(input);
   }

   @Override
   public boolean isReturnValueExpected() {
      return true;
   }

   @Override
   public boolean canBlock() {
      return true;
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
      if (!(obj instanceof ReduceCommand)) {
         return false;
      }
      ReduceCommand other = (ReduceCommand) obj;
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
   public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((taskId == null) ? 0 : taskId.hashCode());
      return result;
   }

   @Override
   public String toString() {
      return "ReduceCommand [keys=" + keys + ", taskId=" + taskId + "]";
   }
}
