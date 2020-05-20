package org.infinispan.cli.converters;

import org.aesh.command.converter.Converter;
import org.aesh.command.converter.ConverterInvocation;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 11.0
 **/
public class NullableIntegerConverter implements Converter<Integer, ConverterInvocation> {
   @Override
   public Integer convert(ConverterInvocation invocation) {
      String input = invocation.getInput();
      return input == null || input.isEmpty() ? null : Integer.parseInt(input);
   }
}
