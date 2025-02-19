package org.infinispan.cdc.configuration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.infinispan.configuration.serializer.ConfigurationSerializerValidator.compareAttributeSets;
import static org.junit.jupiter.api.Named.named;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.io.IOException;
import java.nio.file.Path;
import java.util.stream.Stream;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.configuration.serializer.ConfigurationSerializerValidator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class ChangeDataCaptureConfigurationSerializerTest {

   private final ConfigurationSerializerValidator validator = new ConfigurationSerializerValidator(
         this::compareExtraConfiguration,
         null,
         null
   );

   @ParameterizedTest
   @MethodSource("loadConfigurationFiles")
   public void configurationSerializationTest(ConfigurationSerializerValidator.Parameter parameter) throws IOException {
      validator.validateConfigurationSerialization(parameter);
   }

   @Test
   public void invalidCDCConfiguration() {
      ParserRegistry registry = new ParserRegistry(Thread.currentThread().getContextClassLoader());
      assertThatThrownBy(() -> registry.parseFile("configuration/invalid-cdc.xml"))
            .isInstanceOf(CacheConfigurationException.class)
            .hasMessageContaining("ISPN008029");
   }

   private static Stream<Arguments> loadConfigurationFiles() {
      ParserRegistry registry = new ParserRegistry(Thread.currentThread().getContextClassLoader());
      return Stream.of(
            arguments(named("pooled-complete", new ConfigurationSerializerValidator.Parameter(Path.of("configuration/pooled-cdc.xml"), MediaType.APPLICATION_XML, registry))),
            arguments(named("pooled-file", new ConfigurationSerializerValidator.Parameter(Path.of("configuration/pooled-file-cdc.xml"), MediaType.APPLICATION_XML, registry))),
            arguments(named("simple", new ConfigurationSerializerValidator.Parameter(Path.of("configuration/simple-cdc.xml"), MediaType.APPLICATION_XML, registry))),
            arguments(named("jndi", new ConfigurationSerializerValidator.Parameter(Path.of("configuration/jndi-cdc.xml"), MediaType.APPLICATION_XML, registry))),
            arguments(named("cdi", new ConfigurationSerializerValidator.Parameter(Path.of("configuration/cdi-cdc.xml"), MediaType.APPLICATION_XML, registry)))
      );
   }

   private void compareExtraConfiguration(String name, Configuration configurationBefore, Configuration configurationAfter) {
      ChangeDataCaptureConfiguration before = configurationBefore.module(ChangeDataCaptureConfiguration.class);
      ChangeDataCaptureConfiguration after = configurationAfter.module(ChangeDataCaptureConfiguration.class);

      assertThat(before.enabled()).isEqualTo(after.enabled());
      assertThat(before.foreignKeys()).isEqualTo(after.foreignKeys());
      assertThat(before.table()).isEqualTo(after.table());
      assertThat(before.connectorProperties()).isEqualTo(after.connectorProperties());

      compareAttributeSets(name, before.connectionFactory().attributes(), after.connectionFactory().attributes());
   }
}
