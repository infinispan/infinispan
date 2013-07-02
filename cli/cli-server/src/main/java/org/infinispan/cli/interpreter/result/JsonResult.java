package org.infinispan.cli.interpreter.result;

import org.codehaus.jackson.annotate.JsonTypeInfo;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectMapper.DefaultTyping;

/**
 * JsonResult. Returns the data formatted as JSON.
 *
 * @author tst
 * @since 5.2
 */
public class JsonResult implements Result {
   private Object o;
   private ObjectMapper jsonMapper = new ObjectMapper().enableDefaultTyping(DefaultTyping.NON_FINAL, JsonTypeInfo.As.WRAPPER_OBJECT);

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
