package org.infinispan.ensemble;

import net.killa.kept.KeptConcurrentMap;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import javax.xml.bind.*;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

/**
 *
 * @author otrack
 * @since 4.0
 */
public class ZkManager {

    private static JAXBContext pContext;
    private static ZkManager instance;
    private static String host = "127.0.0.1";
    private static int port = 2181;
    private static int timeout = 2000;

    private ZooKeeper zk;

    private ZkManager() throws IOException {
        zk =  new ZooKeeper(host+":"+Integer.toString(port), timeout, null);
    }

    public static ZkManager getInstance(){
        synchronized (ZkManager.class){
            if(instance==null)
                try {
                    instance = new ZkManager();
                } catch (IOException e) {
                    e.printStackTrace();
                }
        }
        return instance;
    }

    public <K,V> ConcurrentMap<K,V> newConcurrentMap(String name){
        try {
            return new ZkSharedMap<K,V>(
                    new KeptConcurrentMap(zk, "/"+name, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT));
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return null;
    }



    //
    // INNER METHODS
    //

    private static <T> String marshal(T object){
        try{
            StringWriter sw = new StringWriter();
            Marshaller marshaller = pContext.createMarshaller();
            marshaller.setProperty(Marshaller.JAXB_ENCODING, "UTF-8");
            marshaller.marshal(object, sw);
            return sw.toString();
        } catch (PropertyException e) {
            e.printStackTrace();  // TODO: Customise this generated block
        } catch (JAXBException e) {
            e.printStackTrace();  // TODO: Customise this generated block
        }
        return null;
    }

    private static <T> T unmarshal(String s){
        try{
            Unmarshaller unmarshaller = pContext.createUnmarshaller();
            unmarshaller.setProperty(Marshaller.JAXB_ENCODING, "UTF-8");
            StringReader sr = new StringReader(s);
            return (T) unmarshaller.unmarshal(sr);
        } catch (PropertyException e) {
            e.printStackTrace();  // TODO: Customise this generated block
        } catch (JAXBException e) {
            e.printStackTrace();  // TODO: Customise this generated block
        }
        return null;
    }


    //
    // INNER CLASSES
    //

    private static class ZkSharedMap<K,V> implements ConcurrentMap<K,V> {

        private KeptConcurrentMap keptConcurrentMap;

        public ZkSharedMap(KeptConcurrentMap m){
            keptConcurrentMap = m;
        }


        @Override
        public V putIfAbsent(K k, V v) {
            return unmarshal(keptConcurrentMap .put(marshal(k),marshal(v)));
        }

        @Override
        public boolean remove(Object o, Object o2) {
            return keptConcurrentMap.remove(marshal(o),marshal(o2));
        }

        @Override
        public boolean replace(K k, V v, V v2) {
            return keptConcurrentMap.replace(marshal(k),marshal(v),marshal(v2));
        }

        @Override
        public V replace(K k, V v) {
            return unmarshal(keptConcurrentMap.replace(marshal(k),marshal(v)));
        }

        @Override
        public int size() {
            return keptConcurrentMap.size();
        }

        @Override
        public boolean isEmpty() {
            return keptConcurrentMap.isEmpty();
        }

        @Override
        public boolean containsKey(Object o) {
            return keptConcurrentMap.containsKey(marshal(o));
        }

        @Override
        public boolean containsValue(Object o) {
            return keptConcurrentMap.containsValue(marshal(o));
        }

        @Override
        public V get(Object o) {
            return unmarshal(keptConcurrentMap.get(marshal(o)));
        }

        @Override
        public V put(K k, V v) {
            return unmarshal(keptConcurrentMap.put(marshal(k),marshal(v)));
        }

        @Override
        public V remove(Object o) {
            return unmarshal(keptConcurrentMap.remove(marshal(o)));
        }

        @Override
        public void putAll(Map<? extends K, ? extends V> map) {
            throw new RuntimeException("NYI");
        }

        @Override
        public void clear() {
            keptConcurrentMap.clear();
        }

        @Override
        public Set<K> keySet() {
            throw new RuntimeException("NYI");
        }

        @Override
        public Collection<V> values() {
            throw new RuntimeException("NYI");
        }

        @Override
        public Set<Entry<K, V>> entrySet() {
            throw new RuntimeException("NYI");
        }
    }

}
