package org.infinispan.cli.interpreter.statement;

import java.util.List;

import org.infinispan.Cache;
import org.infinispan.cli.interpreter.codec.Codec;
import org.infinispan.cli.interpreter.result.EmptyResult;
import org.infinispan.cli.interpreter.result.Result;
import org.infinispan.cli.interpreter.result.StatementException;
import org.infinispan.cli.interpreter.session.Session;

/**
 * Implements the "rm [cache.]key" statetement which removes the specified key from a cache
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public class RemoveStatement extends CodecAwareStatement {
   final KeyData keyData;
   final Object value;

   public RemoveStatement(List<Option> options, final KeyData key, final Object value) {
      super(options);
      this.keyData = key;
      this.value = value;
   }

   @Override
   public Result execute(Session session) throws StatementException {
      Cache<Object, Object> cache = session.getCache(keyData.getCacheName());
      Codec codec = getCodec(session);
      if (value == null) {
         cache.remove(codec.encodeKey(keyData.getKey()));
      } else {
         cache.remove(codec.encodeKey(keyData.getKey()), codec.encodeValue(value));
      }
      return EmptyResult.RESULT;
   }

}
