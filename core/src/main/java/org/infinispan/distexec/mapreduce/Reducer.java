package org.infinispan.distexec.mapreduce;

import java.io.Serializable;
import java.util.Iterator;

/**
 * Reduces intermediate key/value results from map phase of MapReduceTask. Infinispan distributed
 * execution environment uses one instance of Reducer per execution node.
 * 
 * 
 * @see Mapper
 * @see MapReduceTask
 * 
 * @author Manik Surtani
 * @author Vladimir Blagojevic
 * @author Sanne Grinovero
 * 
 * @since 5.0
 */
public interface Reducer<KOut, VOut> extends Serializable {

   /**
    * Combines/reduces all intermediate values for a particular intermediate key to a single value.
    * <p>
    * 
    */
   VOut reduce(KOut reducedKey, Iterator<VOut> iter);

}
