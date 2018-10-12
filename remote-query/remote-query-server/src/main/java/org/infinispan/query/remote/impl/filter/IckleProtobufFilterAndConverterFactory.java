package org.infinispan.query.remote.impl.filter;

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
@MetaInfServices(ParamKeyValueFilterConverterFactory.class)
@SuppressWarnings("unused")
public final class IckleProtobufFilterAndConverterFactory
      extends AbstractIckleFilterConverterFactory<KeyValueFilterConverter>
      implements ParamKeyValueFilterConverterFactory {

   public static final String FACTORY_NAME = "iteration-filter-converter-factory";

   @Override
   protected KeyValueFilterConverter getFilterConverter(String queryString, Map<String, Object> namedParams) {
      return new IckleBinaryProtobufFilterAndConverter<>(queryString, namedParams);
   }

   @Override
   public boolean binaryParam() {
      return true;
   }
}
