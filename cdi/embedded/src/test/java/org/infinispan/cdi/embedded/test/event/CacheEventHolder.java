package org.infinispan.cdi.embedded.test.event;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.infinispan.notifications.cachemanagerlistener.event.Event;

/**
 * Collects all events from observer and allows to extract them.
 *
 * @author Sebastian Laskawiec
 */
public class CacheEventHolder {

   /**
    * Main data structure of this event holder.
    * <p>
    *    From top to bottom:
    *    <ul>
    *       <li><code>Map<Class, Map></code> - holds Cache annotation as keys and event map as value</li>
    *       <li><code>Map<Class, List></code> - holds Event class as keys and a list of occurred event as value</li>
    *       <li><code>List</code> - holds list of occurred events</li>
    *    </ul>
    * </p>
    */
   private final Map<Class<?>, Map<Class<?>, List<Object>>> eventMap = new HashMap<>();

   private void addEventClass(Class<?> cacheAnnotationClass, Class<?> eventClass) {
      if(!eventMap.containsKey(cacheAnnotationClass)) {
         eventMap.put(cacheAnnotationClass, new HashMap<>());
      }
      if(!eventMap.get(cacheAnnotationClass).containsKey(eventClass)) {
         eventMap.get(cacheAnnotationClass).put(eventClass, new ArrayList<>());
      }
   }

   /**
    * Adds event to this holder.
    *
    * @param cacheAnnotationClass CDI Cache qualifier annotation (like <code>@Cache1</code>).
    * @param eventStaticClass Event static class information (event have generic type information).
    * @param event Event itself.
    * @param <T> Generic information about event type.
    */
   public synchronized <T extends org.infinispan.notifications.cachelistener.event.Event> void addEvent(Class<?> cacheAnnotationClass, Class<T> eventStaticClass, T event) {
      addEventClass(cacheAnnotationClass, eventStaticClass);
      eventMap.get(cacheAnnotationClass).get(eventStaticClass).add(event);
   }

   /**
    * Adds event to this holder.
    *
    * @param cacheAnnotationClass CDI Cache qualifier annotation (like <code>@Cache1</code>).
    * @param eventStaticClass Event static class information (event have generic type information).
    * @param event Event itself.
    * @param <T> Generic information about event type.
    */
   public synchronized <T extends Event> void addEvent(Class<?> cacheAnnotationClass, Class<T> eventStaticClass, T event) {
      addEventClass(cacheAnnotationClass,eventStaticClass);
      eventMap.get(cacheAnnotationClass).get(eventStaticClass).add(event);
   }

   public synchronized void clear() {
      eventMap.clear();
   }

   /**
    * Gets all events based on Cache annotation and class of events.
    *
    * @param cacheAnnotationClass CDI Cache qualifier annotation (like <code>@Cache1</code>).
    * @param eventClass Event class.
    * @param <T> Generic information about event type.
    * @return List of events occurred. Empty list if there was no events.
    */
   public synchronized <T> List<T> getEvents(Class<?> cacheAnnotationClass, Class<T> eventClass) {
      ArrayList<T> toBeReturned = new ArrayList<>();
      Map<Class<?>, List<Object>> eventsMapForGivenCache = eventMap.get(cacheAnnotationClass);
      if(eventsMapForGivenCache == null) {
         return toBeReturned;
      }
      List<Object> events = eventsMapForGivenCache.get(eventClass);
      if(events != null) {
         for (Object event : events) {
            toBeReturned.add(eventClass.cast(event));
         }
      }
      return toBeReturned;
   }
}
