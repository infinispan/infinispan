package org.infinispan.cli.interpreter.result;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * JsonResult. Returns the data formatted as JSON.
 *
 * @author tst
 * @since 5.2
 */
public class JsonResult implements Result {
   private Object o;
   private ObjectMapper jsonMapper = new ObjectMapper().enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL, JsonTypeInfo.As.WRAPPER_OBJECT);

   public JsonResult(Object o) {
      this.o = o;
   }

   @Override
   public String getResult() {
      try {
         return jsonMapper.writeValueAsString(o);
      } catch (Exception e) {
         return e.getMessage();
      }
   }
}
