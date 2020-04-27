package org.infinispan.persistence.jpa;

import org.infinispan.persistence.KeyValueWrapper;
import org.infinispan.persistence.jpa.entity.KeyValueEntity;

/**
 * Wraps the key/value in {@link KeyValueEntity}.
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
public class JpaKeyValueWrapper implements KeyValueWrapper<String, String, KeyValueEntity> {

   public static final JpaKeyValueWrapper INSTANCE = new JpaKeyValueWrapper();

   @Override
   public KeyValueEntity wrap(String key, String value) {
      return new KeyValueEntity(key, value);
   }

   @Override
   public String unwrap(KeyValueEntity object) {
      return object == null ? null : object.getValue();
   }
}
