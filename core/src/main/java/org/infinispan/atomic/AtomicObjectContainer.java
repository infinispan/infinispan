package org.infinispan.atomic;

import javassist.util.proxy.MethodFilter;
import javassist.util.proxy.MethodHandler;
import javassist.util.proxy.ProxyFactory;
import javassist.util.proxy.ProxyObject;
import org.infinispan.Cache;
import org.infinispan.commons.marshall.jboss.GenericJBossMarshaller;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.event.CacheEntryModifiedEvent;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.*;

/**
  * @author Pierre Sutra
 *  @since 6.0
 *
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
    private static final int N_RETRIEVE_HELPERS= 2;
    private static final int CALL_TTIMEOUT_TIME = 3000;
    private static final int RETRIEVE_TTIMEOUT_TIME = 30000;
    private static ExecutorService service = Executors.newCachedThreadPool();

    //
    // OBJECT FIELDS
    //

    private Cache cache;
    private Object object;
    private Class clazz;
    private Object proxy;

    private Boolean withReadOptimization;
    private Set<String> readOptimizationFailedMethods;
    private Method equalsMethod;

    private Object key;
    private Map<Integer,AtomicObjectCallFuture> registeredCalls;
    private int hash;

    private BlockingQueue<AtomicObjectCall> calls;
    private Future<Integer> callHandlerFuture;

    private AtomicObjectCallFuture retrieve_future;
    private ArrayList<AtomicObjectCallInvoke> retrieve_calls;
    private AtomicObjectCallRetrieve retrieve_call;

    public AtomicObjectContainer(Cache c, Class cl, Object k, boolean readOptimization, Method m, boolean forceNew)
            throws IOException, ClassNotFoundException, IllegalAccessException, InstantiationException, InterruptedException, ExecutionException {

        cache = c;
        clazz = cl;
        key = k;

        readOptimizationFailedMethods = new HashSet<String>();
        withReadOptimization = readOptimization;

        equalsMethod = m;

        hash = 0;
        registeredCalls = new ConcurrentHashMap<Integer, AtomicObjectCallFuture>();

        // build the proxy
        MethodHandler handler = new MethodHandler() {
            public Object invoke(Object self, Method m, Method proceed, Object[] args) throws Throwable {

                GenericJBossMarshaller marshaller = new GenericJBossMarshaller();

                synchronized(withReadOptimization){
                    if (withReadOptimization && !readOptimizationFailedMethods.contains(m.getName())) {
                        Object copy = marshaller.objectFromByteBuffer(marshaller.objectToByteBuffer(object));
                        Object ret = doCall(copy,m.getName(),args);
                        if( equalsMethod == null ? copy.equals(object) : equalsMethod.invoke(copy, object).equals(Boolean.TRUE) )
                            return ret;
                        else
                            readOptimizationFailedMethods.add(m.getName());
                    }
                }

                int callID = nextCallID(cache);
                AtomicObjectCallInvoke invoke = new AtomicObjectCallInvoke(callID,m.getName(),args);
                byte[] bb = marshaller.objectToByteBuffer(invoke);
                AtomicObjectCallFuture future = new AtomicObjectCallFuture();
                registeredCalls.put(callID, future);
                cache.put(key, bb);
                log.debug("Called "+invoke+" on object "+key);

                Object ret = future.get(CALL_TTIMEOUT_TIME,TimeUnit.MILLISECONDS);
                registeredCalls.remove(callID);
                if(!future.isDone()){
                    throw new TimeoutException("Unable to execute "+invoke+" on "+key);
                }
                return ret;
            }
        };

        ProxyFactory fact = new ProxyFactory();
        fact.setSuperclass(clazz);
        fact.setFilter(mfilter);
        proxy = fact.createClass().newInstance();
        ((ProxyObject)proxy).setHandler(handler);

        calls = new LinkedBlockingDeque<AtomicObjectCall>();
        AtomicObjectContainerTask callHandler = new AtomicObjectContainerTask();
        callHandlerFuture = service.submit(callHandler);

        // Register
        cache.addListener(this);

        // Build the object
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
            calls.add(call);

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
        callHandlerFuture.cancel(true);
    }

    private void initObject(boolean forceNew) throws IllegalAccessException, InstantiationException {

        if( !forceNew){
            GenericJBossMarshaller marshaller = new GenericJBossMarshaller();
            try {
                AtomicObjectCall persist = (AtomicObjectCall) marshaller.objectFromByteBuffer((byte[]) cache.get(key));
                if(persist instanceof AtomicObjectCallPersist){
                    object = ((AtomicObjectCallPersist)persist).object;
                }else{
                    log.debug("Retrieving object "+key);
                    retrieve_future = new AtomicObjectCallFuture();
                    retrieve_call = new AtomicObjectCallRetrieve(nextCallID(cache));
                    marshaller = new GenericJBossMarshaller();
                    cache.put(key,marshaller.objectToByteBuffer(retrieve_call));
                    retrieve_future.get(RETRIEVE_TTIMEOUT_TIME,TimeUnit.MILLISECONDS);
                    if(!retrieve_future.isDone()) throw new TimeoutException();
                }
                return;
            } catch (Exception e) {
                log.info("Enable to retrieve object " + key + " from the cache.");
            }
        }

        log.info("Object "+key+" is created.");
        object = clazz.newInstance();

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

    /**
     *
     * @param invocation
     * @return true if the operation is local
     * @throws InvocationTargetException
     * @throws IllegalAccessException
     */
    private boolean handleInvocation(AtomicObjectCallInvoke invocation)
            throws InvocationTargetException, IllegalAccessException {
        synchronized(withReadOptimization){
            Object ret = doCall(object,invocation.method,invocation.arguments);
            if(registeredCalls.containsKey(invocation.callID)){
                assert ! registeredCalls.get(invocation.callID).isDone() : "Received twice "+ invocation.callID+" ?";
                registeredCalls.get(invocation.callID).setReturnValue(ret);
                return true;
            }
            return false;
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

    //
    // INNER CLASSES
    //

    private class AtomicObjectContainerTask implements Callable<Integer>{

        private int retrieveRank;

        public AtomicObjectContainerTask(){
            retrieveRank = 0;
        }

        @Override
        public Integer call() throws Exception {

            try {

                while(true){

                    AtomicObjectCall call = calls.take();

                    if (call instanceof AtomicObjectCallInvoke) {

                        if(object != null){

                            AtomicObjectCallInvoke invocation = (AtomicObjectCallInvoke) call;
                            hash+=invocation.callID;
                            if(handleInvocation(invocation))
                                retrieveRank = N_RETRIEVE_HELPERS;
                            else if (retrieveRank > 0)
                                retrieveRank --;
                            else
                                retrieveRank = 0;

                        }else if (retrieve_calls != null) {

                            retrieve_calls.add((AtomicObjectCallInvoke) call);

                        }

                    } else if (call instanceof AtomicObjectCallRetrieve) {

                        if (object != null && retrieveRank > 0) {

                            AtomicObjectCallPersist persist = new AtomicObjectCallPersist(0,object);
                            GenericJBossMarshaller marshaller = new GenericJBossMarshaller();
                            cache.put(key,marshaller.objectToByteBuffer(persist));

                        }else if (retrieve_call != null && retrieve_call.callID == ((AtomicObjectCallRetrieve)call).callID) {

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

                }

            } catch (InterruptedException e) {
                return 0;
            } catch (Exception e) {
                e.printStackTrace();
            }
            return 1;

        }
    }

}