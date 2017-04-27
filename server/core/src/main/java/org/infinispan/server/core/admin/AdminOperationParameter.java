package org.infinispan.server.core.admin;

import java.util.Map;
import java.util.Optional;

/**
 * AdminOperationParameters
 *
 * @author Tristan Tarrant
 * @since 9.1
 */
public enum AdminOperationParameter {
   CACHE_NAME,
   CACHE_TEMPLATE,
   FLAGS;

   public String require(Map<String, String> params) {
      return params.computeIfAbsent(name(), k -> {
         throw new IllegalArgumentException("Required argument '" + k + "' is missing");
      });
   }

   public Optional<String> optional(Map<String, String> params) {
      return Optional.ofNullable(params.get(name()));
   }
}
