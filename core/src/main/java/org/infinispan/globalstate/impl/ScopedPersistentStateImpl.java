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
   public String getProperty(String key) {
      return state.get(key);
   }

   @Override
   public void forEach(BiConsumer<String, String> action) {
      state.forEach(action);
   }

}
