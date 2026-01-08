package org.infinispan.cli.converters;

import org.aesh.command.converter.Converter;
import org.aesh.command.converter.ConverterInvocation;
import org.aesh.command.validator.OptionValidatorException;

public abstract class EnumConverter<T extends Enum<T>> implements Converter<T, ConverterInvocation> {
   protected final Class<T> enumClass;

   protected EnumConverter(Class<T> enumClass) {
      this.enumClass = enumClass;
   }

   @Override
   public T convert(ConverterInvocation ci) throws OptionValidatorException {
      String input = ci.getInput();
      if (input == null || input.isEmpty()) {
         return null;
      }
      return Enum.valueOf(enumClass, input.toUpperCase().replace('-', '_'));
   }
}
