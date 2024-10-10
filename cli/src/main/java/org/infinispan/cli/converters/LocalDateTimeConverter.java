package org.infinispan.cli.converters;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import org.aesh.command.converter.Converter;
import org.aesh.command.converter.ConverterInvocation;

/**
 * Parses and input on the format `<code>04/Sep/2024:13:18:14</code>` to a {@link LocalDateTime}.
 *
 * @author José Bolina
 * @since 15.0
 */
public class LocalDateTimeConverter implements Converter<LocalDateTime, ConverterInvocation> {

   private final DateTimeFormatter dtf = DateTimeFormatter.ofPattern("dd/MMM/yyyy:HH:mm:ss");

   @Override
   public LocalDateTime convert(ConverterInvocation ivk) {
      String input = ivk.getInput();
      if (input == null || input.isEmpty())
         return null;

      return dtf.parse(input, LocalDateTime::from);
   }
}
