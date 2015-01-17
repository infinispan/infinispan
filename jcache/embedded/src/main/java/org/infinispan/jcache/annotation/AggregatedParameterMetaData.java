package org.infinispan.jcache.annotation;

import static java.util.Collections.unmodifiableList;

import java.util.List;

/**
 * Contains all parameters metadata for a method annotated with a cache annotation.
 *
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 * @author Galder Zamarreño
 */
public class AggregatedParameterMetaData {

   private final List<ParameterMetaData> parameters;
   private final List<ParameterMetaData> keyParameters;
   private final ParameterMetaData valueParameter;

   public AggregatedParameterMetaData(List<ParameterMetaData> parameters,
         List<ParameterMetaData> keyParameters,
         ParameterMetaData valueParameter) {

      this.parameters = unmodifiableList(parameters);
      this.keyParameters = unmodifiableList(keyParameters);
      this.valueParameter = valueParameter;
   }

   public List<ParameterMetaData> getParameters() {
      return parameters;
   }

   public List<ParameterMetaData> getKeyParameters() {
      return keyParameters;
   }

   public ParameterMetaData getValueParameter() {
      return valueParameter;
   }

   @Override
   public String toString() {
      return new StringBuilder()
            .append("AggregatedParameterMetaData{")
            .append("parameters=").append(parameters)
            .append(", keyParameters=").append(keyParameters)
            .append(", valueParameter=").append(valueParameter)
            .append('}')
            .toString();
   }
}
