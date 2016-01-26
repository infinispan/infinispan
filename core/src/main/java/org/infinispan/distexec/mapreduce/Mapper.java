package org.infinispan.distexec.mapreduce;

import org.infinispan.Cache;
import org.infinispan.CacheSet;

import java.io.Serializable;

/**
 * Implementation of a Mapper class is a component of a MapReduceTask invoked once for each input
 * entry K,V. Every Mapper instance migrated to an Infinispan node, given a cache entry K,V input
 * pair transforms that input pair into intermediate keys and emits them into Collector provided by
 * Infinispan execution environment. Intermediate results are further reduced using a
 * {@link Reducer}.
 * 
 * 
 * @see Reducer
 * @see MapReduceTask
 * 
 * @author Manik Surtani
 * @author Vladimir Blagojevic
 * @author Sanne Grinovero
 * 
 * @since 5.0
 * @deprecated Map reduce is being replaced by Streams
 * @see Cache#entrySet()
 * @see CacheSet#stream()
 */
public interface Mapper<KIn, VIn, KOut, VOut> extends Serializable {

   /**
    * Invoked once for each input cache entry KIn,VOut pair.
    */
   void map(KIn key, VIn value, Collector<KOut, VOut> collector);
}
