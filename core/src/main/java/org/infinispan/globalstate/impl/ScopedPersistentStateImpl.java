package org.infinispan.globalstate.impl;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.BiConsumer;

import org.infinispan.globalstate.ScopedPersistentState;

/**
 * ScopedPersistentStateImpl.
 *
 * @author Tristan Tarrant
 * @since 8.1
 */
public class ScopedPersistentStateImpl implements ScopedPersistentState {
   private final String scope;
   private final Map<String, String> state;

   public ScopedPersistentStateImpl(String scope) {
      this.scope = scope;
      this.state = new LinkedHashMap<>(); // to preserve order
   }

   @Override
   public String getScope() {
      return scope;
   }

   @Override
   public void setProperty(String key, String value) {
      state.put(key, value);
   }

   @Override
   public void setProperty(String key, int value) {
      setProperty(key, Integer.toString(value));
   }

   @Override
   public int getIntProperty(String key) {
      return Integer.parseInt(state.get(key));
   }

   @Override
   public void setProperty(String key, float f) {
      setProperty(key, Float.toString(f));
   }

   @Override
   public float getFloatProperty(String key) {
      return Float.parseFloat(state.get(key));
   }

   @Override
   public String getProperty(String key) {
      return state.get(key);
   }

   @Override
   public void forEach(BiConsumer<String, String> action) {
      state.forEach(action);
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      ScopedPersistentStateImpl that = (ScopedPersistentStateImpl) o;

      if (scope != null ? !scope.equals(that.scope) : that.scope != null) return false;
      return state != null ? state.equals(that.state) : that.state == null;

   }

   @Override
   public int hashCode() {
      int result = scope != null ? scope.hashCode() : 0;
      result = 31 * result + (state != null ? state.hashCode() : 0);
      return result;
   }

   @Override
   public int getChecksum() {
      int result = scope != null ? scope.hashCode() : 0;
      result += state.entrySet().stream().filter(e -> !e.getKey().startsWith("@")).mapToInt(Map.Entry::hashCode).sum();
      return result;
   }

   @Override
   public boolean containsProperty(String key) {
      return state.containsKey(key);
   }
}
