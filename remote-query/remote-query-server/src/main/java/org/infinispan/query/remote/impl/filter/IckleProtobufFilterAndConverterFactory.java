package org.infinispan.query.remote.impl.filter;

import static org.infinispan.query.remote.impl.filter.IckleFilterConverterUtils.unmarshallParams;
import static org.infinispan.query.remote.impl.filter.IckleFilterConverterUtils.unmarshallQueryString;

import java.util.Map;

import org.infinispan.filter.KeyValueFilterConverter;
import org.infinispan.filter.NamedFactory;
import org.infinispan.filter.ParamKeyValueFilterConverterFactory;
import org.kohsuke.MetaInfServices;


/**
 * Factory for a {@link KeyValueFilterConverter} that operates on binary parameters and produces binary results.
 *
 * @author gustavonalle
 * @since 8.1
 */
@NamedFactory(name = IckleProtobufFilterAndConverterFactory.FACTORY_NAME)
@MetaInfServices
@SuppressWarnings("unused")
public final class IckleProtobufFilterAndConverterFactory implements ParamKeyValueFilterConverterFactory {

   public static final String FACTORY_NAME = "iteration-filter-converter-factory";

   @Override
   public KeyValueFilterConverter getFilterConverter(Object[] params) {
      String queryString = unmarshallQueryString(params);
      Map<String, Object> namedParams = unmarshallParams(params);
      return new IckleBinaryProtobufFilterAndConverter<>(queryString, namedParams);
   }

   @Override
   public boolean binaryParam() {
      return true;
   }
}
