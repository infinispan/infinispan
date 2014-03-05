package org.infinispan.notifications;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Class-level annotation used to annotate an object as being a valid cache listener.  Used with the {@link
 * org.infinispan.Cache#addListener(Object)} and related APIs. <p/> Note that even if a class is annotated with this
 * annotation, it still needs method-level annotation (such as {@link org.infinispan.notifications.cachemanagerlistener.annotation.CacheStarted})
 * to actually receive notifications. <p/> Objects annotated with this annotation - listeners - can be attached to a
 * running {@link org.infinispan.Cache} so users can be notified of {@link org.infinispan.Cache} events. <p/> <p/> There can
 * be multiple methods that are annotated to receive the same event, and a method may receive multiple events by using a
 * super type. </p> <p/> <h4>Delivery Semantics</h4> <p/> An event is delivered immediately after the respective
 * operation, but before the underlying cache call returns. For this reason it is important to keep listener processing
 * logic short-lived. If a long running task needs to be performed, it's recommended to use another thread. </p> <p/>
 * <h4>Transactional Semantics</h4> <p/> Since the event is delivered during the actual cache call, the transactional
 * outcome is not yet known. For this reason, <i>events are always delivered, even if the changes they represent are
 * discarded by their containing transaction</i>. For applications that must only process events that represent changes
 * in a completed transaction, {@link org.infinispan.notifications.cachelistener.event.TransactionalEvent#getGlobalTransaction()}
 * can be used, along with {@link org.infinispan.notifications.cachelistener.event.TransactionCompletedEvent#isTransactionSuccessful()}
 * to record events and later process them once the transaction has been successfully committed. Example 4 demonstrates
 * this. </p> <p/> <h4>Threading Semantics</h4> <p/> A listener implementation must be capable of handling concurrent
 * invocations. Local notifications reuse the calling thread; remote notifications reuse the network thread. </p> <p/>
 * Since notifications reuse the calling or network thread, it is important to realise that if your listener
 * implementation blocks or performs a long-running task, the original caller which triggered the cache event may block
 * until the listener callback completes.  It is therefore a good idea to use the listener to be notified of an event
 * but to perform any long running tasks in a separate thread so as not to block the original caller. </p> <p/> In
 * addition, any locks acquired for the operation being performed will still be held for the callback.  This needs to be
 * kept in mind as locks may be held longer than necessary or intended to and may cause deadlocking in certain
 * situations.  See above paragraph on long-running tasks that should be run in a separate thread. </p> <b>Note</b>:
 * The <tt>sync</tt> parameter on this annotation defaults to <tt>true</tt>
 * which provides the above semantics.  Alternatively, if you set <tt>sync</tt> to <tt>false</tt>, then invocations are
 * made in a <i>separate</i> thread, which will not cause any blocking on the caller or network thread.  The separate
 * thread is taken from a pool, which can be configured using {@link org.infinispan.config.GlobalConfiguration#setAsyncListenerExecutorProperties(java.util.Properties)}
 * and {@link org.infinispan.config.GlobalConfiguration#setAsyncListenerExecutorFactoryClass(String)}.
 * <p/>
 * <b>Summary of Notification Annotations</b> <table border="1" cellpadding="1" cellspacing="1" summary="Summary of
 * notification annotations"> <tr> <th bgcolor="#CCCCFF" align="left">Annotation</th> <th bgcolor="#CCCCFF"
 * align="left">Event</th> <th bgcolor="#CCCCFF" align="left">Description</th> </tr> <tr> <td valign="top">{@link
 * org.infinispan.notifications.cachemanagerlistener.annotation.CacheStarted}</td> <td valign="top">{@link
 * org.infinispan.notifications.cachemanagerlistener.event.CacheStartedEvent}</td> <td valign="top">A cache was
 * started</td> </tr> <tr> <td valign="top">{@link org.infinispan.notifications.cachemanagerlistener.annotation.CacheStopped}</td>
 * <td valign="top">{@link org.infinispan.notifications.cachemanagerlistener.event.CacheStoppedEvent}</td> <td
 * valign="top">A cache was stopped</td> </tr> <tr> <td valign="top">{@link org.infinispan.notifications.cachelistener.annotation.CacheEntryModified}</td>
 * <td valign="top">{@link org.infinispan.notifications.cachelistener.event.CacheEntryModifiedEvent}</td> <td
 * valign="top">A cache entry was modified</td> </tr> <tr> <td valign="top">{@link
 * org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated}</td> <td valign="top">{@link
 * org.infinispan.notifications.cachelistener.event.CacheEntryCreatedEvent}</td> <td valign="top">A cache entry was
 * created</td> </tr> <tr> <td valign="top">{@link org.infinispan.notifications.cachelistener.annotation.CacheEntryRemoved}</td>
 * <td valign="top">{@link org.infinispan.notifications.cachelistener.event.CacheEntryRemovedEvent}</td> <td valign="top">A
 * cache entry was removed</td> </tr> <tr> <td valign="top">{@link org.infinispan.notifications.cachelistener.annotation.CacheEntryVisited}</td>
 * <td valign="top">{@link org.infinispan.notifications.cachelistener.event.CacheEntryVisitedEvent}</td> <td valign="top">A
 * cache entry was visited</td> </tr> <tr> <td valign="top">{@link org.infinispan.notifications.cachelistener.annotation.CacheEntryLoaded}</td>
 * <td valign="top">{@link org.infinispan.notifications.cachelistener.event.CacheEntryLoadedEvent}</td> <td valign="top">A
 * cache entry was loaded</td> </tr> <tr> <td valign="top">{@link org.infinispan.notifications.cachelistener.annotation.CacheEntriesEvicted}</td>
 * <td valign="top">{@link org.infinispan.notifications.cachelistener.event.CacheEntriesEvictedEvent}</td> <td valign="top">A
 * cache entries were evicted</td> </tr> <tr> <td valign="top">{@link org.infinispan.notifications.cachelistener.annotation.CacheEntryActivated}</td>
 * <td valign="top">{@link org.infinispan.notifications.cachelistener.event.CacheEntryActivatedEvent}</td> <td
 * valign="top">A cache entry was activated</td> </tr> <tr> <td valign="top">{@link
 * org.infinispan.notifications.cachelistener.annotation.CacheEntryPassivated}</td> <td valign="top">{@link
 * org.infinispan.notifications.cachelistener.event.CacheEntryPassivatedEvent}</td> <td valign="top">One or more cache entries were
 * passivated</td> </tr> <tr> <td valign="top">{@link org.infinispan.notifications.cachemanagerlistener.annotation.ViewChanged}</td>
 * <td valign="top">{@link org.infinispan.notifications.cachemanagerlistener.event.ViewChangedEvent}</td> <td
 * valign="top">A view change event was detected</td> </tr> <tr> <td valign="top">{@link
 * org.infinispan.notifications.cachelistener.annotation.TransactionRegistered}</td> <td valign@="top">{@link
 * org.infinispan.notifications.cachelistener.event.TransactionRegisteredEvent}</td> <td valign="top">The cache has started
 * to participate in a transaction</td> </tr> <tr> <td valign="top">{@link org.infinispan.notifications.cachelistener.annotation.TransactionCompleted}</td>
 * <td valign=@"top">{@link org.infinispan.notifications.cachelistener.event.TransactionCompletedEvent}</td> <td
 * valign="top">The cache has completed its participation in a transaction</td> </tr> <tr> <td valign="top">{@link
 * org.infinispan.notifications.cachelistener.annotation.CacheEntryInvalidated}</td> <td valign=@"top">{@link
 * org.infinispan.notifications.cachelistener.event.CacheEntryInvalidatedEvent}</td> <td valign="top">A cache entry was
 * invalidated by a remote cache.  Only if cache mode is INVALIDATION_SYNC or INVALIDATION_ASYNC.</td> </tr>
 * <p/>
 * </table>
 * <p/>
 * <h4>Example 1 - Method receiving a single event</h4>
 * <pre>
 *    &#064;Listener
 *    public class SingleEventListener
 *    {
 *       &#064;CacheStarted
 *       public void doSomething(Event event)
 *       {
 *          System.out.println(&quot;Cache started.  Details = &quot; + event);
 *       }
 *    }
 * </pre>
 * <p/>
 * <h4>Example 2 - Method receiving multiple events</h4>
 * <pre>
 *    &#064;Listener
 *    public class MultipleEventListener
 *    {
 *       &#064;CacheStarted
 *       &#064;CacheStopped
 *       public void doSomething(Event event)
 *       {
 *          if (event.getType() == Event.Type.CACHE_STARTED)
 *             System.out.println(&quot;Cache started.  Details = &quot; + event);
 *          else if (event.getType() == Event.Type.CACHE_STOPPED)
 *             System.out.println(&quot;Cache stopped.  Details = &quot; + event);
 *       }
 *    }
 * </pre>
 * <p/>
 * <h4>Example 3 - Multiple methods receiving the same event</h4>
 * <pre>
 *    &#064;Listener
 *    public class SingleEventListener
 *    {
 *       &#064;CacheStarted
 *       public void handleStart(Event event)
 *       {
 *          System.out.println(&quot;Cache started&quot;);
 *       }
 * <p/>
 *       &#064;CacheStarted
 *       &#064;CacheStopped
 *       &#064;CacheBlocked
 *       &#064;CacheUnblocked
 *       &#064;ViewChanged
 *       public void logEvent(Event event)
 *       {
 *          logSystem.logEvent(event.getType());
 *       }
 *    }
 * </pre>
 * <p/>
 * <p/>
 * <b>Example 4 - Processing only events with a committed transaction.</b>
 * <p/>
 * <pre>
 *    &#064;Listener
 *    public class EventHandler
 *    {
 *       private ConcurrentMap&lt;GlobalTransaction, Queue&lt;Event&gt;&gt; map = new ConcurrentHashMap&lt;GlobalTransaction, Queue&lt;Event&gt;&gt;();
 *
 *       &#064;TransactionRegistered
 *       public void startTransaction(TransactionRegisteredEvent event)
 *       {
 *          map.put(event.getGlobalTransaction(), new ConcurrentLinkedQueue&lt;Event&gt;());
 *       }
 * 
 *       &#064;CacheEntryCreated
 *       &#064;CacheEntryModified
 *       &#064;CacheEntryRemoved
 *       public void addEvent(TransactionalEvent event)
 *       {
 *          map.get(event.getGlobalTransaction()).add(event);
 *       }
 *  
 *       &#064;TransactionCompleted
 *       public void endTransaction(TransactionCompletedEvent event)
 *       {
 *          Queue&lt;Event&gt; events = map.get(event.getGlobalTransaction());
 *          map.remove(event.getGlobalTransaction());
 *    
 *          System.out.println("Ended transaction " + event.getGlobalTransaction().getId());
 *    
 *          if(event.isTransactionSuccessful())
 *          {
 *             for(Event e : events)
 *             {
 *                System.out.println("Event " + e);
 *             }
 *          }
 *       }
 *    }
 * </pre>
 *
 * @author <a href="mailto:manik@jboss.org">Manik Surtani</a>
 * @author Jason T. Greene
 * @see org.infinispan.notifications.cachemanagerlistener.annotation.CacheStarted
 * @see org.infinispan.notifications.cachemanagerlistener.annotation.CacheStopped
 * @see org.infinispan.notifications.cachelistener.annotation.CacheEntryModified
 * @see org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated
 * @see org.infinispan.notifications.cachelistener.annotation.CacheEntryRemoved
 * @see org.infinispan.notifications.cachelistener.annotation.CacheEntryVisited
 * @see org.infinispan.notifications.cachelistener.annotation.CacheEntryLoaded
 * @see org.infinispan.notifications.cachelistener.annotation.CacheEntriesEvicted
 * @see org.infinispan.notifications.cachelistener.annotation.CacheEntryActivated
 * @see org.infinispan.notifications.cachelistener.annotation.CacheEntryPassivated
 * @see org.infinispan.notifications.cachemanagerlistener.annotation.ViewChanged
 * @see org.infinispan.notifications.cachelistener.annotation.TransactionCompleted
 * @see org.infinispan.notifications.cachelistener.annotation.TransactionRegistered
 * @see org.infinispan.notifications.cachelistener.annotation.CacheEntryInvalidated
 * @see org.infinispan.notifications.cachelistener.annotation.DataRehashed
 * @see org.infinispan.notifications.cachelistener.annotation.TopologyChanged
 * @since 4.0
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface Listener {
   /**
    * Specifies whether callbacks on any class annotated with this annotation happens synchronously (in the caller's
    * thread) or asynchronously (using a separate thread).  Defaults to <tt>true</tt>.
    *
    * @return true if the expectation is that callbacks are called using the caller's thread; false if they are to be
    *         made in a separate thread.
    * @since 4.0
    */
   boolean sync() default true;

   /**
    * Specifies whether the event should be fired on the primary data owner of the affected key, or all nodes that see
    * the update.  In the case of replication, this would be the coordinator.
    *
    * @return true if the expectation is that only the primary data owner will fire the event, false if all nodes that
    *         see the update fire the event.
    *
    *  @since 5.3
    */
   boolean primaryOnly() default false;

   /**
    * Defines whether the annotated listener is clustered or not.
    * Important: Clustered listener can only be notified for @CacheEntryRemoved, @CacheEntryCreated and
    * @CacheEntryModified events.
    * @return true if the expectation is that this listener is to be a cluster listener, as in it will receive
    *         all notifications for data modifications
    * @since 7.0
    */
   boolean clustered() default false;

   /**
    * If set to true then the entire existing state within the cluster is
    * evaluated. For existing matches of the value, an @CacheEntryCreated event is triggered against the listener
    * during registration.  This is only supported if the listener is also
    * {@link org.infinispan.notifications.Listener#clustered()}.
    * <p>
    * If using a distributed clustered cache it is possible to retrieve new events before the initial transfer is
    * completed.  This is handled since only new events are queued until the segment it belongs to is completed
    * for iteration.  This also will help reduce memory strain since a distributed clustered listener will need
    * to eventually retrieve all values from the cache.
    * @return true if the expectation is that when the listener is installed that all of the current data is sent
    *         as new events to the listener before receiving new events
    * @since 7.0
    **/
   boolean includeCurrentState() default false;
}
