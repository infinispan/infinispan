package org.infinispan.distexec.mapreduce;

/**
 * OutputCollector is the intermediate key/value result data output collector given to each {@link Mapper}
 * 
 * @see Mapper#map(Object, Object, Collector)
 * 
 * @author Mircea Markus
 * @author Sanne Grinovero
 * @since 5.0
 */
public interface Collector<K, V> {

   /**
    * Intermediate key/value callback used by {@link Mapper} implementor
    * 
    * @param key
    *           intermediate key
    * @param value
    *           intermediate value
    */
   void emit(K key, V value);

}
