package org.infinispan.cli.interpreter.statement;

import java.util.List;

import org.infinispan.Cache;
import org.infinispan.cli.interpreter.codec.Codec;
import org.infinispan.cli.interpreter.result.EmptyResult;
import org.infinispan.cli.interpreter.result.Result;
import org.infinispan.cli.interpreter.result.StatementException;
import org.infinispan.cli.interpreter.session.Session;

/**
 *
 * EvictStatement evicts an entry from the cache
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public class EvictStatement extends CodecAwareStatement {
   final KeyData keyData;

   public EvictStatement(List<Option> options, KeyData key) {
      super(options);
      this.keyData = key;
   }

   @Override
   public Result execute(Session session) throws StatementException {
      Cache<Object, Object> cache = session.getCache(keyData.getCacheName());
      Codec codec = getCodec(session);
      cache.getAdvancedCache().evict(codec.encodeKey(keyData.getKey()));
      return EmptyResult.RESULT;
   }

}
