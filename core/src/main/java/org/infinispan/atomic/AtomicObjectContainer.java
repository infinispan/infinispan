package org.infinispan.atomic;

import javassist.util.proxy.MethodFilter;
import javassist.util.proxy.MethodHandler;
import javassist.util.proxy.ProxyFactory;
import javassist.util.proxy.ProxyObject;
import org.infinispan.Cache;
import org.infinispan.InvalidCacheUsageException;
import org.infinispan.commons.marshall.jboss.GenericJBossMarshaller;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.event.CacheEntryModifiedEvent;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
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
    private Map<Integer,AtomicObjectCallFuture> registeredCalls;
    private int hash;

    private AtomicObjectCallFuture retrieve_future;
    private ArrayList<AtomicObjectCallInvoke> retrieve_calls;
    private AtomicObjectCallRetrieve retrieve_call;

    public AtomicObjectContainer(Cache c, Class cl, Object k, boolean readOptimization, Method m, boolean forceNew)
            throws IOException, ClassNotFoundException, IllegalAccessException, InstantiationException, InterruptedException, ExecutionException {

        cache = c;
        clazz = cl;
        key = k;
        withReadOptimization = readOptimization;
        equalsMethod = m;

        hash = 0;
        registeredCalls = new ConcurrentHashMap<Integer, AtomicObjectCallFuture>();

        // build the proxy
        MethodHandler handler = new MethodHandler() {
            public Object invoke(Object self, Method m, Method proceed, Object[] args) throws Throwable {

                GenericJBossMarshaller marshaller = new GenericJBossMarshaller();

                if (withReadOptimization) {
                    Object copy = marshaller.objectFromByteBuffer(marshaller.objectToByteBuffer(object));
                    Object ret = doCall(copy,m.getName(),args);
                    if( equalsMethod == null ? copy.equals(object) : equalsMethod.invoke(copy, object).equals(Boolean.TRUE) )
                        return ret;
                }

                int callID = nextCallID(cache);
                AtomicObjectCallInvoke invoke = new AtomicObjectCallInvoke(callID,m.getName(),args);
                byte[] bb = marshaller.objectToByteBuffer(invoke);
                cache.put(key, bb);
                AtomicObjectCallFuture future = new AtomicObjectCallFuture();
                registeredCalls.put(callID, future);
                Object ret = future.get();
                registeredCalls.remove(callID);
                log.debug("Called "+invoke+" on object "+key);

                return ret;
            }
        };

        ProxyFactory fact = new ProxyFactory();
        fact.setSuperclass(clazz);
        fact.setFilter(mfilter);
        proxy = fact.createClass().newInstance();
        ((ProxyObject)proxy).setHandler(handler);

        // Register
        cache.addListener(this);

        // Build object
        initObject(forceNew);

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

            GenericJBossMarshaller marshaller = new GenericJBossMarshaller();
            byte[] bb = (byte[]) event.getValue();
            AtomicObjectCall call = (AtomicObjectCall) marshaller.objectFromByteBuffer(bb);

            log.debug("Received " + call+ " from " + event.getCache().toString());

            if (call instanceof AtomicObjectCallInvoke) {

                if(object != null){

                    AtomicObjectCallInvoke invocation = (AtomicObjectCallInvoke) call;

                    if (registeredCalls.containsKey(invocation.callID) && !event.isOriginLocal()) // an event can be received twice if it is local
                        return;

                    hash+=invocation.callID;

                    handleInvocation(invocation);

                }else if (retrieve_calls != null) {

                    retrieve_calls.add((AtomicObjectCallInvoke) call);

                }

            } else if (call instanceof AtomicObjectCallRetrieve) {

                if (object != null) {

                    AtomicObjectCallPersist persist = new AtomicObjectCallPersist(0,object);
                    cache.put(key,marshaller.objectToBuffer(persist));

                }else if (retrieve_call.callID == ((AtomicObjectCallRetrieve)call).callID) {

                    assert retrieve_calls == null;
                    retrieve_calls = new ArrayList<AtomicObjectCallInvoke>();

                }

            } else { // AtomicObjectCallPersist

                if (object == null && retrieve_calls != null)  {
                    object = ((AtomicObjectCallPersist)call).object;
                    for(AtomicObjectCallInvoke invocation : retrieve_calls){
                        handleInvocation(invocation);
                    }
                   retrieve_future.setReturnValue(null);
                }

            }

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public synchronized void dispose(boolean keepPersistent)
            throws IOException, InterruptedException {
        if (!keepPersistent) {
            cache.remove(key);
        } else {
            GenericJBossMarshaller marshaller = new GenericJBossMarshaller();
            AtomicObjectCallPersist persist = new AtomicObjectCallPersist(0,object);
            cache.put(key,marshaller.objectToByteBuffer(persist));
        }

    }

    private void initObject(boolean forceNew)
            throws InvalidCacheUsageException, IOException, ClassNotFoundException, IllegalAccessException, InstantiationException, InterruptedException, ExecutionException {

        if( cache.containsKey(key) && ! forceNew){

            GenericJBossMarshaller marshaller = new GenericJBossMarshaller();
            AtomicObjectCall persist = null;
            persist = (AtomicObjectCallPersist) marshaller.objectFromByteBuffer((byte[]) cache.get(key));

            if(persist instanceof AtomicObjectCallPersist){

                object = ((AtomicObjectCallPersist)persist).object;

            }else{

                if (object==null ) {

                    if (!cache.containsKey(key))
                        throw new InvalidCacheUsageException("Unable to initizalize object stored "+key);

                    log.debug("Retrieving object "+key);

                    retrieve_future = new AtomicObjectCallFuture();
                    retrieve_call = new AtomicObjectCallRetrieve(nextCallID(cache));
                    marshaller = new GenericJBossMarshaller();
                    cache.put(key,marshaller.objectToByteBuffer(retrieve_call));
                    retrieve_future.get();

                }

            }
        }else{
            log.debug("A new object is created.");
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

    private void handleInvocation(AtomicObjectCallInvoke invocation)
            throws InvocationTargetException, IllegalAccessException {
        Object ret = doCall(object,invocation.method,invocation.arguments);
        if(registeredCalls.containsKey(invocation.callID)){
            assert ! registeredCalls.get(invocation.callID).isDone() : "Received twice "+ invocation.callID+" ?";
            registeredCalls.get(invocation.callID).setReturnValue(ret);
        }
    }


    //
    // HELPERS
    //

    private static int nextCallID(Cache c){
        return ThreadLocalRandom.current().nextInt()+c.hashCode();
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

}