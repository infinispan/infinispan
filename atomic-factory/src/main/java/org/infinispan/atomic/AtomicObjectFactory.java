package org.infinispan.atomic;

import org.infinispan.Cache;
import org.infinispan.InvalidCacheUsageException;
import org.infinispan.atomic.container.Container;
import org.infinispan.atomic.container.ContainerSignature;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


/**
 * @author Pierre Sutra
 * @since 7.0
 */
public class AtomicObjectFactory {

   //
   // CLASS FIELDS
   //
   private static Log log = LogFactory.getLog(AtomicObjectFactory.class);
   private static Map<Cache,AtomicObjectFactory> factories = new HashMap<>();
   public synchronized static AtomicObjectFactory forCache(Cache cache){
      if(!factories.containsKey(cache))
         factories.put(cache, new AtomicObjectFactory(cache));
      return factories.get(cache);
   }
   protected static final int MAX_CONTAINERS=1000;// 0 means no limit
   public static final Map<Class,List<String>> updateMethods;
   static{
      updateMethods = new HashMap<>();

      updateMethods.put(List.class, new ArrayList<String>());
      updateMethods.get(List.class).add("add");
      updateMethods.get(List.class).add("addAll");

      updateMethods.put(Set.class, new ArrayList<String>());
      updateMethods.get(Set.class).add("add");
      updateMethods.get(Set.class).add("addAll");

      updateMethods.put(Map.class, new ArrayList<String>());
      updateMethods.get(Map.class).add("put");
      updateMethods.get(Map.class).add("putAll");
   };



   //
   // OBJECT FIELDS
   //
   private Cache cache;
   private Map<ContainerSignature,Container> registeredContainers;
   private int maxSize;
   private final ExecutorService evictionExecutor = Executors.newCachedThreadPool();


   /**
    *
    * Returns an object factory built on top of cache <i>c</i> with a bounded amount <i>m</i> of
    * containers in it. Upon the removal of a container, the object is stored persistently in the cache.
    *
    * @param c it must be synchronous.and transactional (with autoCommit set to true, its default value).
    * @param m max amount of containers kept by this factory.
    * @throws InvalidCacheUsageException
    */
   public AtomicObjectFactory(Cache<Object, Object> c, int m) throws InvalidCacheUsageException{
      cache = c;
      maxSize = m;
      registeredContainers= new LinkedHashMap<ContainerSignature,Container>(){
         @Override
         protected boolean removeEldestEntry(final java.util.Map.Entry<ContainerSignature,Container> eldest) {
            if(maxSize!=0 && this.size() >= maxSize){
               evictionExecutor.submit(new Callable<Void>() {
                  @Override
                  public Void call() throws IOException {
                     try {
                        log.debug("Disposing " + eldest.getValue().toString());
                        eldest.getValue().dispose(false);
                     } catch (Exception e) {
                        e.printStackTrace();
                     }
                     return null;
                  }
               });
               return true;
            }
            return false;
         }
      };
      log = LogFactory.getLog(this.getClass());
   }

   /**
    *
    * Return an AtomicObjectFactory built on top of cache <i>c</i>.
    *
    * @param c a cache,  it must be synchronous.and transactional (with autoCommit set to true, its default value).
    */
   public AtomicObjectFactory(Cache<Object, Object> c) throws InvalidCacheUsageException{
      this(c,MAX_CONTAINERS);
   }


   /**
    *
    * Returns an atomic object of class <i>clazz</i>.
    * The class of this object must be initially serializable, as well as all the parameters of its methods.
    * Furthermore, the class must be deterministic.
    *
    * @param clazz a class object
    * @param key to use in order to store the object.
    * @return an object of the class <i>clazz</i>
    * @throws InvalidCacheUsageException
    */
   public synchronized <T> T getInstanceOf(Class<T> clazz, Object key)
         throws InvalidCacheUsageException{
      return getInstanceOf(clazz, key, false);
   }

   /**
    *
    * Returns an object of class <i>clazz</i>.
    * The class of this object must be initially serializable, as well as all the parameters of its methods.
    * Furthermore, the class must be deterministic.
    *
    * The object is atomic if <i>withReadOptimization</i> equals false; otherwise it is sequentially consistent..
    * In more details, if <i>withReadOptimization</i>  is set, every call to the object is first executed locally on a copy of the object, and in case
    * the call does not modify the state of the object, the value returned is the result of this tentative execution.
    *
    * @param clazz a class object
    * @param key the key to use in order to store the object.
    * @param withReadOptimization set the read optimization on/off.
    * @return an object of the class <i>clazz</i>
    * @throws InvalidCacheUsageException
    */
   public <T> T getInstanceOf(Class<T> clazz, Object key, boolean withReadOptimization)
         throws InvalidCacheUsageException{
      return getInstanceOf(clazz, key, withReadOptimization, null, true);
   }

   /**
    *
    * Returns an object of class <i>clazz</i>.
    * The class of this object must be initially serializable, as well as all the parameters of its methods.
    * Furthermore, the class must be deterministic.
    *
    * The object is atomic if <i>withReadOptimization</i> equals false; otherwise it is sequentially consistent..
    * In more details, if <i>withReadOptimization</i>  is set, every call to the object is executed locally on a copy of the object, and in case
    * the call does not modify the state of the object, the value returned is the result of this tentative execution.
    * If the method <i>equalsMethod</i>  is not null, it overrides the default <i>clazz.equals()</i> when testing that the state of the object and
    * its copy are identical.
    *
    * @param clazz a class object
    * @param key the key to use in order to store the object.
    * @param withReadOptimization set the read optimization on/off.
    * @param equalsMethod overriding the default <i>clazz.equals()</i>.
    * @param forceNew force the creation of the object, even if it exists already in the cache
    * @return an object of the class <i>clazz</i>
    * @throws InvalidCacheUsageException
    */
   public <T> T getInstanceOf(Class<T> clazz, Object key, boolean withReadOptimization, Method equalsMethod, boolean forceNew, Object ... initArgs)
         throws InvalidCacheUsageException{

      if( !(Serializable.class.isAssignableFrom(clazz))){
         throw new InvalidCacheUsageException("The object must be serializable.");
      }

      ContainerSignature signature = new ContainerSignature(clazz,key);
      Container container;

      try{

         synchronized (registeredContainers){
            container = registeredContainers.get(signature);
         }

         if( container==null){

            List<String> methods = Collections.EMPTY_LIST;

            if (Updatable.class.isAssignableFrom(clazz)) {

               methods = new ArrayList<String>();
               for(Method m : clazz.getDeclaredMethods()){
                  if (m.isAnnotationPresent(Update.class))
                     methods.add(m.getName());
               }

            }else{

               for(Class c : updateMethods.keySet()){
                  if (c.isAssignableFrom(clazz)) {
                     methods = updateMethods.get(c);
                     break;
                  }
               }

               if (methods.isEmpty()) {
                  methods = new ArrayList<String>();
                  for(Method m : clazz.getDeclaredMethods()){
                     methods.add(m.getName());
                  }
               }

            }
            container = new Container(cache, clazz, key, withReadOptimization, forceNew,methods, initArgs);
            synchronized (registeredContainers){
               if(registeredContainers.containsKey(signature)){
                  container.dispose(false);
               }else{
                  registeredContainers.put(signature, container);
               }
            }
         }

      } catch (Exception e){
         e.printStackTrace();
         throw new InvalidCacheUsageException(e.getCause());
      }

      return (T) container.getProxy();

   }

   /**
    * Remove the object stored at <i>key</i>from the local state.
    * If flag <i>keepPersistent</i> is set, a persistent copy of the current state of the object is also stored in the cache.
    *
    * @param clazz a class object
    * @param key the key to use in order to store the object.
    * @param keepPersistent indicates that a persistent copy is stored in the cache or not.
    */
   public void disposeInstanceOf(Class clazz, Object key, boolean keepPersistent)
         throws InvalidCacheUsageException {

      ContainerSignature signature = new ContainerSignature(clazz,key);
      log.debug("Disposing "+signature);
      Container container;
      synchronized (registeredContainers){
         container = registeredContainers.get(signature);
         if( container == null )
            return;
         if( ! container.getClazz().equals(clazz) )
            throw new InvalidCacheUsageException("The object is not of the right class.");
         registeredContainers.remove(signature);
      }

      try{
         container.dispose(keepPersistent);
      }catch (Exception e){
         e.printStackTrace();
         throw new InvalidCacheUsageException("Error while disposing object "+key);
      }

   }

}
