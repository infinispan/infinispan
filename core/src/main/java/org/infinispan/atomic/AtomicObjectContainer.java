package org.infinispan.atomic;

import javassist.util.proxy.MethodFilter;
import javassist.util.proxy.MethodHandler;
import javassist.util.proxy.ProxyFactory;
import javassist.util.proxy.ProxyObject;
import org.infinispan.Cache;
import org.infinispan.InvalidCacheUsageException;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.event.CacheEntryModifiedEvent;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.*;

/**
 * TODO check that method matching in doCall is unique (this should be the case accordingly to the way Java handles polymorphism).
 *
  * @author Pierre Sutra
 */
@Listener
public class AtomicObjectContainer {

    //
    // CLASS FIELDS
    //
    private static final int MAGIC_PERSIST = 0;
    private static final MethodFilter mfilter = new MethodFilter() {
        @Override
        public boolean isHandled(Method arg0) {
            return true;
        }
    };
    private static Log log = LogFactory.getLog(AtomicObjectContainer.class);

    //
    // OBJECT FIELDS
    //
    private Cache cache;
    private Object object;
    private Class clazz;
    private Object proxy;
    private boolean withReadOptimization;
    private Method equalsMethod;
    private Object key;
    private Map<Integer,FutureAtomicObjectCall> registeredCalls;
    private int hash;

    public AtomicObjectContainer(Cache c, Class cl, Object k, boolean readOptimization, Method m, boolean forceNew)
            throws IOException, ClassNotFoundException, IllegalAccessException, InstantiationException {

        cache = c;
        clazz = cl;
        key = k;
        withReadOptimization = readOptimization;
        equalsMethod = m;

        hash = 0;
        registeredCalls = new ConcurrentHashMap<Integer, FutureAtomicObjectCall>();

        // build the proxy
        MethodHandler handler = new MethodHandler() {
            public Object invoke(Object self, Method m, Method proceed, Object[] args) throws Throwable {

                int callId = nextCallID(cache);

                if(registeredCalls.containsKey(callId))
                    throw new InvalidCacheUsageException("Access must be sequential per thread");

                ObjectOutputStream oos;
                ByteArrayOutputStream baos;
                ObjectInputStream ois;
                ByteArrayInputStream bais;

                if(withReadOptimization){
                    baos = new ByteArrayOutputStream();
                    oos = new ObjectOutputStream(baos);
                    oos.writeObject(key);
                    oos.flush();
                    bais = new ByteArrayInputStream(baos.toByteArray());
                    ois = new ObjectInputStream(bais);
                    Object copy = ois.readObject();
                    Object ret = doCall(copy,m.getName(),args);
                    if( equalsMethod == null ? copy.equals(key) : equalsMethod.invoke(copy, object).equals(Boolean.TRUE) )
                        return ret;
                }

                baos = new ByteArrayOutputStream();
                oos = new ObjectOutputStream(baos);
                oos.writeInt(callId);
                oos.writeObject(m.getName());
                oos.writeObject(args);
                byte [] data = baos.toByteArray();
                ByteBuffer bb = ByteBuffer.allocate(data.length);
                bb.put(data);
                bb.flip();

                FutureAtomicObjectCall future = new FutureAtomicObjectCall();
                registeredCalls.put(callId, future);

                cache.put(key, bb.array());

                Object ret = future.get();
                registeredCalls.remove(callId);

                log.debug("Call (" + callId + ") " + m.getName() + ":" + ret);

                return ret;
            }
        };

        ProxyFactory fact = new ProxyFactory();
        fact.setSuperclass(clazz);
        fact.setFilter(mfilter);
        proxy = fact.createClass().newInstance();
        ((ProxyObject)proxy).setHandler(handler);

        // Build object
        initObject(forceNew);

        cache.addListener(this);

    }

    /**
     * Internal use of the listener API.
     *
     * @param event of class CacheEntryModifiedEvent
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    @CacheEntryModified
    @Deprecated
    public synchronized void onCacheModification(CacheEntryModifiedEvent<Object,Object> event){

        if( !event.getKey().equals(key) )
            return;

        if(event.isPre())
            return;

        try {

            byte[] bb = (byte[]) event.getValue();
            ByteArrayInputStream bais = new ByteArrayInputStream(bb);
            ObjectInputStream ois = new ObjectInputStream(bais);
            int callId = ois.readInt();

            if(registeredCalls.containsKey(callId) && !event.isOriginLocal()) // an event can be received twice if it is local
                return;

            log.debug("Received call with ID=" + callId + " from " + event.getCache().toString());

            if(callId == MAGIC_PERSIST){
                return;
            }

            String method = (String)ois.readObject();
            Object[] args = (Object[])ois.readObject();
            Object ret = doCall(object,method,args);

            hash+=callId;

            if(registeredCalls.containsKey(callId)){
               assert ! registeredCalls.get(callId).isDone() : "Received twice "+ callId+" ?";
               registeredCalls.get(callId).setReturnValue(ret);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public synchronized void dispose(boolean keepPersistent) throws IOException {
        if (!keepPersistent) {
            cache.remove(key);
        } else {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeInt(MAGIC_PERSIST);
            oos.writeObject(object);
            byte [] data = baos.toByteArray();
            ByteBuffer bb = ByteBuffer.allocate(data.length);
            bb.put(data);
            bb.flip();
            cache.put(key, bb.array());
        }

    }

    private void initObject(boolean forceNew) throws IOException, ClassNotFoundException, IllegalAccessException, InstantiationException {
        if( cache.containsKey(key) && ! forceNew){
            ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream((byte[]) cache.get(key)));
            int callID = ois.readInt();
            if( callID != MAGIC_PERSIST )
                throw new InvalidCacheUsageException("Unable to retrieve persistent state.");
            object = ois.readObject();
        }else{
            object = clazz.newInstance();
        }

    }

    public Object getProxy(){
        return proxy;
    }

    public Class getClazz(){
        return clazz;
    }

    /**
     * @return a hash of the order in which the calls where executed.
     */
    @Override
    public int hashCode(){
        return hash;
    }


    //
    // INNER CLASSES & HELPERS
    //

    private static int nextCallID(Cache c){
        int ret =  ThreadLocalRandom.current().nextInt()+c.hashCode();
        if( ret == MAGIC_PERSIST ) ret++ ;
        return ret;
    }

    private static Object doCall(Object obj, String method, Object[] args) throws InvocationTargetException, IllegalAccessException {
        boolean isFound = false;
        Object ret = null;
        for (Method m : obj .getClass().getMethods()) { // only public methods (inherited and not)
            if (method.equals(m.getName())) {
                boolean isAssignable = true;
                Class[] argsTypes = m.getParameterTypes();
                if(argsTypes.length == args.length){
                    for(int i=0; i<argsTypes.length; i++){
                        if( !argsTypes[i].isAssignableFrom(args[i].getClass()) ){
                            isAssignable = false;
                            break;
                        }
                    }
                }
                if(!isAssignable)
                    continue;

                ret = m.invoke(obj, args);
                isFound = true;
                break;
            }
        }

        if(!isFound)
            throw new IllegalStateException("Method "+method+" not found.");

        return ret;
    }

    private class FutureAtomicObjectCall implements Future<Object> {

        private Object ret;
        private int state; // 0 => init, 1 => done, -1 => cancelled

        public FutureAtomicObjectCall(){
            ret = null;
            state = 0;
        }

        public void setReturnValue(Object r){
            synchronized (this) {

                if (state == -1)
                    return ;

                if (ret == null) {
                    ret = r;
                    state = 1;
                    this.notifyAll();
                    return;
                }
            }

            throw new IllegalStateException("Unreachable code");
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            synchronized (this) {
                if (state != 0)
                    return false;
                state = -1;
                if (mayInterruptIfRunning)
                    this.notifyAll();
            }
            return true;
        }

        @Override
        public Object get() throws InterruptedException, ExecutionException {
            synchronized (this) {
                if (state == 0)
                    this.wait();
            }
            return (state == -1) ? null : ret;
        }

        @Override
        public Object get(long timeout, TimeUnit unit)
                throws InterruptedException, ExecutionException,
                TimeoutException {
            synchronized (this) {
                if (state == 0)
                    this.wait(timeout);
            }
            return (state == -1) ? null : ret;
        }

        @Override
        public boolean isCancelled() {
            return state == -1;
        }

        @Override
        public boolean isDone() {
            return ret != null;
        }

    }

}