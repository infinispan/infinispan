package org.infinispan.configuration.serializer;

import static org.assertj.core.api.Assertions.assertThat;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.distribution.ch.impl.HashFunctionPartitioner;
import org.infinispan.distribution.ch.impl.RESPHashFunctionPartitioner;
import org.testng.annotations.Test;

@Test(testName = "configuration.serializer.ConfigurationSerializerTest", groups = "functional")
public class ConfigurationSerializerTest extends AbstractConfigurationSerializerTest {
}
