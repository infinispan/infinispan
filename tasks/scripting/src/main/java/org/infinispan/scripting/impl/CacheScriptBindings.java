package org.infinispan.scripting.impl;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import javax.script.Bindings;

/**
 * CacheScriptBindings.
 *
 * @author Tristan Tarrant
 * @since 7.2
 */
public class CacheScriptBindings implements Bindings {
   private final Bindings systemBindings;
   private final Bindings userBindings;

   public CacheScriptBindings(Bindings systemBindings, Bindings userBindings) {
      this.systemBindings = systemBindings;
      this.userBindings = userBindings;
   }

   @Override
   public boolean containsKey(Object key) {
      return systemBindings.containsKey(key) || userBindings.containsKey(key);
   }

   @Override
   public Object get(Object key) {
      if (systemBindings.containsKey(key)) {
         return systemBindings.get(key);
      } else {
         return userBindings.get(key);
      }
   }

   @Override
   public int size() {
      return userBindings.size() + systemBindings.size();
   }

   @Override
   public boolean isEmpty() {
      return userBindings.isEmpty() && systemBindings.isEmpty();
   }

   @Override
   public boolean containsValue(Object value) {
      return systemBindings.containsValue(value) || userBindings.containsValue(value);
   }

   @Override
   public void clear() {
      userBindings.clear();
   }

   @Override
   public Set<String> keySet() {
      return userBindings.keySet();//TODO: join with systemBindings
   }

   @Override
   public Collection<Object> values() {
      return userBindings.values();//TODO: join with systemBindings
   }

   @Override
   public Set<java.util.Map.Entry<String, Object>> entrySet() {
      return userBindings.entrySet(); //TODO: join with systemBindings
   }

   @Override
   public Object put(String name, Object value) {
      if (systemBindings.containsKey(name)) {
         throw new IllegalArgumentException();
      } else {
         return userBindings.put(name, value);
      }
   }

   @Override
   public void putAll(Map<? extends String, ? extends Object> toMerge) {
      //FIXME implement me
   }

   @Override
   public Object remove(Object key) {
      if (systemBindings.containsKey(key)) {
         throw new IllegalArgumentException();
      } else {
         return userBindings.remove(key);
      }
   }
}
