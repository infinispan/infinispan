package org.infinispan.cli.interpreter.statement;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.List;

import org.infinispan.Cache;
import org.infinispan.cli.interpreter.codec.Codec;
import org.infinispan.cli.interpreter.result.JsonResult;
import org.infinispan.cli.interpreter.result.Result;
import org.infinispan.cli.interpreter.result.StatementException;
import org.infinispan.cli.interpreter.result.StringResult;
import org.infinispan.cli.interpreter.session.Session;

/**
 *
 * Implementation of the "get" statement
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public class GetStatement extends CodecAwareStatement {
   final KeyData keyData;

   public GetStatement(List<Option> options, KeyData key) {
      super(options);
      this.keyData = key;
   }

   @Override
   public Result execute(Session session) throws StatementException {
      Cache<Object, Object> cache = session.getCache(keyData.getCacheName());
      Object key = keyData.key;
      Codec codec = getCodec(session);
      Object value = cache.get(codec.encodeKey(key));
      if (value == null) {
         return new StringResult("null");
      } else {
         Object decoded = codec.decodeValue(value);
         if (decoded instanceof byte[]) {
            return new StringResult(new String((byte[]) decoded), UTF_8);
         } else if (decoded instanceof String) {
            return new StringResult((String) decoded);
         } else if (decoded.getClass().isPrimitive()) {
            return new StringResult(decoded.toString());
         } else {
            return new JsonResult(decoded);
         }
      }
   }

}
