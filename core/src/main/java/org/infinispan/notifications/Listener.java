package org.infinispan.notifications;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.infinispan.configuration.global.GlobalConfiguration;

/**
 * Class-level annotation used to annotate an object as being a valid cache listener.  Used with the {@link
 * org.infinispan.Cache#addListener(Object)} and related APIs.
 * <p/> Note that even if a class is annotated with this
 * annotation, it still needs method-level annotation (such as {@link org.infinispan.notifications.cachemanagerlistener.annotation.CacheStarted})
 * to actually receive notifications.
 * <p/> Objects annotated with this annotation - listeners - can be attached to a
 * running {@link org.infinispan.Cache} so users can be notified of {@link org.infinispan.Cache} events.
 * <p/> <p/> There can be multiple methods that are annotated to receive the same event, and a method may receive
 * multiple events by using a super type.
 * <h4>Delivery Semantics</h4>
 * An event is delivered immediately after the respective
 * operation, sometimes before as well, but must complete before the underlying cache call returns. For this reason it
 * is important to keep listener processing
 * logic short-lived. If a long running task needs to be performed, it's recommended to invoke this in a non blocking
 * way or to use an async listener.
 * <h4>Transactional Semantics</h4>
 * Since the event is delivered during the actual cache call, the transactional
 * outcome is not yet known. For this reason, <i>events are always delivered, even if the changes they represent are
 * discarded by their containing transaction</i>. For applications that must only process events that represent changes
 * in a completed transaction, {@link org.infinispan.notifications.cachelistener.event.TransactionalEvent#getGlobalTransaction()}
 * can be used, along with {@link org.infinispan.notifications.cachelistener.event.TransactionCompletedEvent#isTransactionSuccessful()}
 * to record events and later process them once the transaction has been successfully committed. Example 4 demonstrates
 * this.
 * <h4>Listener Modes</h4>
 * A listener can be configured to run in two different modes: sync or async.
 * <p>The first, non-blocking, is a mode where the listener is notified in the invoking thread. Operations in this mode
 * should be used when either the listener operation is expected to complete extremely fast or when the operation can be
 * performed in a non-blocking manner by returning a CompletionStage to delay
 * the operation until the stage is complete. This mode is the default mode, overrided by the {@link Listener#sync()}
 * property. A method is non blocking if it declares that it returns a {@link java.util.concurrent.CompletionStage} or
 * one of its subtypes. Note that the stage may return a value, but it will be ignored. The user <b>must</b> be very
 * careful that no blocking or long running operation is done while in a sync listener as it can cause thread
 * starvation. You should instead use your own thread pool to execute the blocking or long running operation and
 * return a {@link java.util.concurrent.CompletionStage} signifying when it is complete.
 * <p>The second, async, is pretty much identical to sync except that the original operation can continue and complete
 * while the listener is notified in a different thread. Listeners that throw exceptions are always logged and are not
 * propagated to the user. This mode is enabled when the listener has specified <code>sync</code> as <b>false</b> and
 * the return value is always ignored.
 * <h4>Locking semantics</h4>
 * The sync mode will guarantee that listeners are notified for mutations on the same key sequentially, since
 * the lock for the key will be held when notifying the listener. Async however can have events notified in any order
 * so they should not be used when this ordering is required. If however the notification thread pool size is limited
 * to one, this will provide ordering for async events, but the throughput of async events may be reduced.
 * <p>Because the key lock is held for the entire execution of sync listeners (until the completion stage is done),
 * sync listeners should be as short as possible. Acquiring additional locks is not recommended, as it could lead to deadlocks
 * <h4>Threading Semantics</h4>
 * A listener implementation must be capable of handling concurrent
 * invocations. Local sync notifications reuse the calling thread; remote sync notifications reuse the
 * network thread. If a listener is async, it will be invoked in the notification thread pool.
 * <h4>Notification Pool</h4>
 * Async events are made in a <i>separate</i> notification thread, which will not cause any blocking on the
 * caller or network thread.  The separate thread for async listeners is taken from a pool, which can be
 * configured using {@link GlobalConfiguration#listenerThreadPool()}. The
 * default values can be found in the {@link org.infinispan.factories.KnownComponentNames} class.
 * <h4>Clustered Listeners</h4>
 * Listeners by default are classified as a local listener. That is that they only receive events that are generated
 * on the node to which they were registered. They also receive pre and post notification events. A clustered listener,
 * configured with <code>clustered=true</code>, receives a subset of events but from any node that
 * generated the given event, not just the one they were registered on. The events that a clustered listener can receive are:
 * {@link org.infinispan.notifications.cachelistener.event.CacheEntryCreatedEvent},
 * {@link org.infinispan.notifications.cachelistener.event.CacheEntryModifiedEvent},
 * {@link org.infinispan.notifications.cachelistener.event.CacheEntryRemovedEvent} and
 * {@link org.infinispan.notifications.cachelistener.event.CacheEntryExpiredEvent}.
 * For performance reasons, a clustered listener only receives post events.
 * <h4>Summary of Notification Annotations</h4>
 * <table border="1" cellpadding="1" cellspacing="1" summary="Summary of notification annotations">
 *    <tr>
 *       <th bgcolor="#CCCCFF" align="left">Annotation</th>
 *       <th bgcolor="#CCCCFF" align="left">Event</th>
 *       <th bgcolor="#CCCCFF" align="left">Description</th>
 *    </tr>
 *    <tr>
 *       <td valign="top">{@link org.infinispan.notifications.cachemanagerlistener.annotation.CacheStarted}</td>
 *       <td valign="top">{@link org.infinispan.notifications.cachemanagerlistener.event.CacheStartedEvent}</td>
 *       <td valign="top">A cache was started</td>
 *    </tr>
 *    <tr>
 *       <td valign="top">{@link org.infinispan.notifications.cachemanagerlistener.annotation.CacheStopped}</td>
 *       <td valign="top">{@link org.infinispan.notifications.cachemanagerlistener.event.CacheStoppedEvent}</td>
 *       <td valign="top">A cache was stopped</td>
 *    </tr>
 *    <tr>
 *       <td valign="top">{@link org.infinispan.notifications.cachelistener.annotation.CacheEntryModified}</td>
 *       <td valign="top">{@link org.infinispan.notifications.cachelistener.event.CacheEntryModifiedEvent}</td>
 *       <td valign="top">A cache entry was modified</td>
 *    </tr>
 *    <tr>
 *       <td valign="top">{@link org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated}</td>
 *       <td valign="top">{@link org.infinispan.notifications.cachelistener.event.CacheEntryCreatedEvent}</td>
 *       <td valign="top">A cache entry was created</td>
 *    </tr>
 *    <tr>
 *       <td valign="top">{@link org.infinispan.notifications.cachelistener.annotation.CacheEntryRemoved}</td>
 *       <td valign="top">{@link org.infinispan.notifications.cachelistener.event.CacheEntryRemovedEvent}</td>
 *       <td valign="top">A cache entry was removed</td>
 *    </tr>
 *    <tr>
 *       <td valign="top">{@link org.infinispan.notifications.cachelistener.annotation.CacheEntryExpired}</td>
 *       <td valign="top">{@link org.infinispan.notifications.cachelistener.event.CacheEntryExpiredEvent}</td>
 *       <td valign="top">A cache entry was expired</td>
 *    </tr>
 *    <tr>
 *       <td valign="top">{@link org.infinispan.notifications.cachelistener.annotation.CacheEntryVisited}</td>
 *       <td valign="top">{@link org.infinispan.notifications.cachelistener.event.CacheEntryVisitedEvent}</td>
 *       <td valign="top">A cache entry was visited</td>
 *    </tr>
 *    <tr>
 *       <td valign="top">{@link org.infinispan.notifications.cachelistener.annotation.CacheEntryLoaded}</td>
 *       <td valign="top">{@link org.infinispan.notifications.cachelistener.event.CacheEntryLoadedEvent}</td>
 *       <td valign="top">A cache entry was loaded</td>
 *    </tr>
 *    <tr>
 *       <td valign="top">{@link org.infinispan.notifications.cachelistener.annotation.CacheEntriesEvicted}</td>
 *       <td valign="top">{@link org.infinispan.notifications.cachelistener.event.CacheEntriesEvictedEvent}</td>
 *       <td valign="top">A cache entries were evicted</td>
 *    </tr>
 *    <tr>
 *       <td valign="top">{@link org.infinispan.notifications.cachelistener.annotation.CacheEntryActivated}</td>
 *       <td valign="top">{@link org.infinispan.notifications.cachelistener.event.CacheEntryActivatedEvent}</td>
 *       <td valign="top">A cache entry was activated</td>\
 *    </tr>
 *    <tr>
 *       <td valign="top">{@link org.infinispan.notifications.cachelistener.annotation.CacheEntryPassivated}</td>
 *       <td valign="top">{@link org.infinispan.notifications.cachelistener.event.CacheEntryPassivatedEvent}</td>
 *       <td valign="top">One or more cache entries were passivated</td>
 *    </tr>
 *    <tr>
 *       <td valign="top">{@link org.infinispan.notifications.cachemanagerlistener.annotation.ViewChanged}</td>
 *       <td valign="top">{@link org.infinispan.notifications.cachemanagerlistener.event.ViewChangedEvent}</td>
 *       <td valign="top">A view change event was detected</td>
 *    </tr>
 *    <tr>
 *       <td valign="top">{@link org.infinispan.notifications.cachelistener.annotation.TransactionRegistered}</td>
 *       <td valign@="top">{@link org.infinispan.notifications.cachelistener.event.TransactionRegisteredEvent}</td>
 *       <td valign="top">The cache has started to participate in a transaction</td>
 *    </tr>
 *    <tr>
 *       <td valign="top">{@link org.infinispan.notifications.cachelistener.annotation.TransactionCompleted}</td>
 *       <td valign=@"top">{@link org.infinispan.notifications.cachelistener.event.TransactionCompletedEvent}</td>
 *       <td valign="top">The cache has completed its participation in a transaction</td>
 *    </tr>
 *    <tr>
 *       <td valign="top">{@link org.infinispan.notifications.cachelistener.annotation.CacheEntryInvalidated}</td>
 *       <td valign=@"top">{@link org.infinispan.notifications.cachelistener.event.CacheEntryInvalidatedEvent}</td>
 *       <td valign="top">A cache entry was invalidated by a remote cache.  Only if cache mode is INVALIDATION_SYNC or INVALIDATION_ASYNC.</td>
 *    </tr>
 * <p/>
 * </table>
 * <p/>
 * <h4>Example 1 - Method receiving a single event, sync</h4>
 * <pre>
 *    &#064;Listener
 *    public class SingleEventListener
 *    {
 *       &#064;CacheStarted
 *       public CompletionStage&lt;Void&gt; doSomething(Event event)
 *       {
 *          System.out.println(&quot;Cache started.  Details = &quot; + event);
 *          return null;
 *       }
 *    }
 * </pre>
 * <p/>
 * <h4>Example 2 - Method receiving multiple events - sync</h4>
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
 * <h4>Example 3 - Multiple methods receiving the same event - async</h4>
 * <pre>
 *    &#064;Listener(sync=false)
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
 * <b>Example 4 - Processing only events with a committed transaction - sync/non-blocking</b>
 * <p/>
 * <pre>
 *    &#064;Listener
 *    public class EventHandler
 *    {
 *       private ConcurrentMap&lt;GlobalTransaction, Queue&lt;Event&gt;&gt; map = new ConcurrentHashMap&lt;GlobalTransaction, Queue&lt;Event&gt;&gt;();
 *
 *       &#064;TransactionRegistered
 *       public CompletionStage&lt;Void&gt; startTransaction(TransactionRegisteredEvent event)
 *       {
 *          map.put(event.getGlobalTransaction(), new ConcurrentLinkedQueue&lt;Event&gt;());
 *          return null;
 *       }
 *
 *       &#064;CacheEntryCreated
 *       &#064;CacheEntryModified
 *       &#064;CacheEntryRemoved
 *       public CompletionStage&lt;Void&gt; addEvent(TransactionalEvent event)
 *       {
 *          map.get(event.getGlobalTransaction()).add(event);'
 *          return null;
 *       }
 *
 *       &#064;TransactionCompleted
 *       public CompletionStage&lt;Void&gt; endTransaction(TransactionCompletedEvent event)
 *       {
 *          Queue&lt;Event&gt; events = map.get(event.getGlobalTransaction());
 *          map.remove(event.getGlobalTransaction());
 *
 *          System.out.println("Ended transaction " + event.getGlobalTransaction().getId());
 *
 *          if(event.isTransactionSuccessful())
 *          {
 *             // Lets say we want to remotely log the events for the transaction - if this has an async or non blocking
 *             // API you can use that and not block the thread and wait until it returns to complete the Stage.
 *             CompletionStage&lt;Void&gt; stage = performRemoteEventUpdateNonBlocking(events);
 *             return stage;
 *          } else {
 *             return null;
 *          }
 *       }
 *    }
 * </pre>
 *
 * @author <a href="mailto:manik@jboss.org">Manik Surtani</a>
 * @author Jason T. Greene
 * @author William Burns
 * @see org.infinispan.notifications.cachemanagerlistener.annotation.CacheStarted
 * @see org.infinispan.notifications.cachemanagerlistener.annotation.CacheStopped
 * @see org.infinispan.notifications.cachelistener.annotation.CacheEntryModified
 * @see org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated
 * @see org.infinispan.notifications.cachelistener.annotation.CacheEntryRemoved
 * @see org.infinispan.notifications.cachelistener.annotation.CacheEntryExpired
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
 * @see org.infinispan.notifications.cachelistener.annotation.PartitionStatusChanged
 * @see org.infinispan.notifications.cachelistener.annotation.PersistenceAvailabilityChanged
 * @since 4.0
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface Listener {
   /**
    * Specifies whether callbacks on any class annotated with this annotation happens synchronously or asynchronously.
    * Please see the appropriate section on the {@link Listener} class for more details.
    * Defaults to <tt>true</tt>.
    *
    * @return true if the expectation is that the operation waits until the callbacks complete before continuing;
    * false if the operation can continue immediately.
    * @since 4.0
    */
   boolean sync() default true;

   /**
    * Specifies whether the event should be fired on the primary data owner of the affected key, or all nodes that see
    * the update.
    * <p>
    * Note that is value is ignored when {@link org.infinispan.notifications.Listener#clustered()} is true.
    * @return true if the expectation is that only the primary data owner will fire the event, false if all nodes that
    *         see the update fire the event.
    *
    *  @since 5.3
    */
   boolean primaryOnly() default false;

   /**
    * Defines whether the annotated listener is clustered or not.
    * Important: Clustered listener can only be notified for
    * {@link org.infinispan.notifications.cachelistener.annotation.CacheEntryRemoved},
    * {@link org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated},
    * {@link org.infinispan.notifications.cachelistener.annotation.CacheEntryRemoved}
    * and {@link org.infinispan.notifications.cachelistener.annotation.CacheEntryExpired} events.
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

   /**
    * Returns the type of observation level this listener defines.
    * @return the observation level of this listener
    * @see Observation
    * @since 7.2
    */
   Observation observation() default Observation.BOTH;


   /**
    * Enumeration that defines when a listener event can be observed. A listener can receive an event before and/or
    * after an operation has occurred.  If the user wishes to be notified just before the operation completes
    * the listener should observe using {@link Observation#PRE} level.  If the user wishes to be notified only
    * after the operation completes the listener should observe using {@link Observation#POST} level.  If the user
    * wishes to have an event before and after they should observe using {@link Observation#BOTH} level.
    */
   enum Observation {
      /**
       * Observation level used to only be notified of an operation before it completes
       */
      PRE() {
         @Override
         public boolean shouldInvoke(boolean pre) {
            return pre;
         }
      },
      /**
       * Observation level used to only be notified of an operation after it has completed
       */
      POST() {
         @Override
         public boolean shouldInvoke(boolean pre) {
            return !pre;
         }
      },
      /**
       * Observation level used to be notified of an operation before and after it occurs
       */
      BOTH() {
         @Override
         public boolean shouldInvoke(boolean pre) {
            return true;
         }
      };

      public abstract boolean shouldInvoke(boolean pre);
   }
}
