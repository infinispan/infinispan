package org.infinispan.atomic.sharded.collections;

import org.infinispan.atomic.Updatable;
import org.infinispan.atomic.AtomicObjectFactory;
import org.infinispan.atomic.Update;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.*;



/**
 *
 * A sorted map abstraction implemented via an ordered forest of trees.
 * The ordered forest of trees is stored in variable <i>forest</i>.
 * Each trees is a shared object implemented with the atomic object factory.
 * It contains at most <i>threshold</i> objects.
 *
 * @author Pierre Sutra
 * @since 7.0
 *
 */
public class ShardedTreeMap<K extends Comparable<K>,V> extends Updatable implements SortedMap<K, V>
{

    private static Log log = LogFactory.getLog(ShardedTreeMap.class);
    private final static int DEFAULT_THRESHOLD = 1000;

    private SortedMap<K,TreeMap<K,V>> forest; // the ordered forest
    private int threshold; // how many entries are stored before creating a new tree in the forest.

    public ShardedTreeMap(){
        forest = new TreeMap<K, TreeMap<K,V>>();
        threshold = DEFAULT_THRESHOLD;
    }

    public ShardedTreeMap(Integer threshhold){
        assert threshhold>=1;
        forest = new TreeMap<K, TreeMap<K,V>>();
        this.threshold = threshhold;
    }

    @Override
    public SortedMap<K, V> subMap(K k, K k2) {
        SortedMap<K,V> result = new TreeMap<K, V>();
        for(K key : forest.keySet()){
            if (key.compareTo(k2) > 0)
                break;
            allocateTree(key);
            result.putAll(forest.get(key).subMap(k, k2));
        }
        unallocateTrees();
        return result;
    }

    @Override
    public SortedMap<K, V> headMap(K toKey) {
        SortedMap<K,V> result = new TreeMap<K, V>();
        for(K key : forest.keySet()){
            if (key.compareTo(toKey) > 0)
                break;
            allocateTree(key);
            result.putAll(forest.get(key).headMap(toKey));
        }
        unallocateTrees();
        return result;
    }

    @Override
    public SortedMap<K, V> tailMap(K fromKey) {
        SortedMap<K,V> result = new TreeMap<K, V>();
        for(K key : forest.keySet()){
            allocateTree(key);
            result.putAll(forest.get(key).tailMap(fromKey));
        }
        unallocateTrees();
        return result;
    }

    @Override
    public K firstKey() {
        if(forest.isEmpty())
            return null;
        allocateTree(forest.firstKey());
        assert !forest.get(forest.firstKey()).isEmpty() : forest.toString();
        K ret = forest.get(forest.firstKey()).firstKey();
        unallocateTrees();
        return ret;
    }

    @Override
    public K lastKey() {
        if(forest.isEmpty())
            return null;
        K last = forest.lastKey();
        allocateTree(last);
        if (!forest.get(last).isEmpty()) {
            last = forest.get(last).lastKey();
        }
        unallocateTrees();
        return last;
    }

    @Override
    public int size() {
        int ret = 0;
        for(K v: forest.keySet()){
            allocateTree(v);
            ret+= forest.get(v).size();
        }
        unallocateTrees();
        return ret;
    }

    @Override
    public boolean isEmpty() {
        return forest.isEmpty();
    }

    @Override
    public V get(Object o) {
        if (forest.isEmpty())
            return null;
        K last = forest.lastKey();
        TreeMap<K,V> treeMap = allocateTree(last);
        assert !treeMap.isEmpty();
        V ret = treeMap.lastEntry().getValue();
        unallocateTrees();
        return ret;
    }

    @Update
    @Override
    public V put(K k, V v) {
        V ret = doPut(k,v);
        unallocateTrees();
        return ret;
    }

    @Override
    public boolean equals(Object o) {
        return this == o;
    }

    @Override
    public int hashCode() {
        return forest.hashCode();
    }

    @Update
    @Override
    public void putAll(Map<? extends K, ? extends V> map) {
        if (forest.isEmpty()){
            TreeMap<K,V> treeMap = new TreeMap<K, V>(map);
            int split = treeMap.size()/this.threshold+1;
            K beg = treeMap.firstKey();
            for(int i=0; i<split; i++){
                TreeMap<K,V> sub = allocateTree(beg);
                forest.put(beg,sub);
                TreeMap<K,V> toAdd = new TreeMap<K, V>();
                for(K k : treeMap.tailMap(beg).keySet()){
                    if(toAdd.size()==threshold){
                        beg = k;
                        break;
                    }
                    toAdd.put(k,treeMap.get(k));
                }
                sub.putAll(toAdd);
            }
        }else{
            for(K k : map.keySet()){
                doPut(k, map.get(k));
            }
        }
        unallocateTrees();
    }

    @Override
    public String toString(){
        TreeMap<K,V> all = new TreeMap<K, V>();
        for(K key : forest.keySet()){
            allocateTree(key);
            all.putAll(forest.get(key));
        }
        unallocateTrees();
        return all.toString();
    }

    //
    // MARSHALLING
    //

    @Override
    public void writeExternal(ObjectOutput objectOutput) throws IOException {
        objectOutput.writeObject(new ArrayList<K>(forest.keySet()));
    }

    @Override
    public void readExternal(ObjectInput objectInput) throws IOException, ClassNotFoundException {
        forest = new TreeMap<K, TreeMap<K,V>>();
        for( K k : (List<K>)objectInput.readObject()){
            forest.put(k,null);
        }
    }


    //
    // HELPERS
    //

    private V doPut(K k, V v){
        log.debug("adding " + k + "=" + v);

        V ret;
        K key;
        TreeMap<K,V> tree;
        SortedMap<K,TreeMap<K,V>> headMap;

        // 1 - Find the tree where to add (k,v)
        headMap = forest.headMap(k);
        if (!headMap.isEmpty() && allocateTree(headMap.lastKey()).size() < threshold)
            key = headMap.lastKey();
        else
            key = k;

        // 2 - add (k,v)
        tree = allocateTree(key);
        ret = tree.put(k,v);

        log.debug("in tree "+key+" -> "+tree);

        // 3 - update the forest if needed
        // 3.1 - change the GLB element in the tree
        if (key!=k && tree.firstKey().equals(k)) {
            forest.remove(key);
            forest.put(k,tree);
        }

        // 3.2 -split the tree if needed
        if (tree.size() > threshold) {
            Entry<K,V> entry = tree.lastEntry();
            tree.remove(entry.getKey());
            put(entry.getKey(),entry.getValue());
        }

        return ret;

    }


    private TreeMap<K,V> allocateTree(K k){
        log.debug("Allocating "+k);
        if(forest.get(k)==null){
            TreeMap treeMap = AtomicObjectFactory.forCache(this.getCache()).getInstanceOf(
                    TreeMap.class, this.getKey().toString()+":"+k.toString(), true, null, false);
            forest.put(k, treeMap);
            log.debug("... done ");
        }
        return forest.get(k);
    }

    private void unallocateTrees(){
        List<K> toUnallocate = new ArrayList<K>();
        for(K k : forest.keySet()){
            if(forest.get(k)!=null){
                toUnallocate.add(k);
            }
        }
        for(K k : toUnallocate){
            log.debug("Unallocate "+k);
            AtomicObjectFactory.forCache(this.getCache()).disposeInstanceOf(
                    TreeMap.class, this.getKey().toString()+":"+k.toString(), true);
            forest.put(k,null);
        }

    }

    @Override
    public boolean containsKey(Object o) {
        for(K k : forest.keySet()){
            allocateTree(k);
            if(forest.get(k).containsKey(o))
                return true;
        }
        return false;
    }

    //
    // NOT YET IMPLEMENTED
    //

    @Override
    public V remove(Object o) {
        throw new UnsupportedOperationException("to be implemented");
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException("to be implemented");
    }

    @Override
    public Set<K> keySet() {
        throw new UnsupportedOperationException("to be implemented");
    }

    @Override
    public Collection<V> values() {
        throw new UnsupportedOperationException("to be implemented");
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        throw new UnsupportedOperationException("to be implemented");
    }

    @Override
    public boolean containsValue(Object o) {
        throw new UnsupportedOperationException("to be implemented");
    }

    @Override
    public Comparator<? super K> comparator() {
        throw new UnsupportedOperationException("to be implemented");
    }

}
