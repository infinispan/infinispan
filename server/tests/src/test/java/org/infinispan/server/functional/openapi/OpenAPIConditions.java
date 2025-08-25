package org.infinispan.server.functional.openapi;

import org.assertj.core.api.Condition;
import org.infinispan.client.openapi.ApiException;

public class OpenAPIConditions {
   public static Condition<Throwable> notFound() {
      return new Condition<>(
            exception -> ((ApiException) exception).getCode() == 404,
            "is not found (404)"
      );
   }
}

