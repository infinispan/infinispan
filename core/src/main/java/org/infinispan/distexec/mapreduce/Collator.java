package org.infinispan.distexec.mapreduce;

import java.util.Map;

/**
 * Collator collates results from Reducers executed on Infinispan cluster and assembles a final
 * result returned to an invoker of MapReduceTask.
 * 
 * 
 * @see MapReduceTask#execute(Collator)
 * @see MapReduceTask#executeAsynchronously(Collator)
 * @see Reducer
 * 
 * @author Manik Surtani
 * @author Vladimir Blagojevic
 * 
 * @since 5.0
 */
public interface Collator<KOut, VOut, R> {

   /**
    * Collates all reduced results and returns R to invoker of distributed task.
    * 
    * @return final result of distributed task computation
    */
   R collate(Map<KOut, VOut> reducedResults);
}
