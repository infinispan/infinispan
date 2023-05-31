package org.infinispan.configuration;

import java.util.Map;
import java.util.Properties;

import org.infinispan.commons.configuration.io.ConfigurationReader;
import org.infinispan.commons.configuration.io.ConfigurationResourceResolvers;
import org.infinispan.commons.configuration.io.ConfigurationWriter;
import org.infinispan.commons.configuration.io.NamingStrategy;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.io.StringBuilderWriter;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.testng.annotations.Test;

@Test(groups = "functional", testName= "configuration.ConfigurationConversionTest")
public class ConfigurationConversionTest {
    @Test
    public void testAuthorizationRoles() {
        String xml = "<distributed-cache owners=\"2\" mode=\"SYNC\" statistics=\"false\">\n" +
                "          <security>\n" +
                "            <authorization />\n" +
                "          </security>\n" +
                "        <encoding media-type=\"application/x-java-serialized-object\" />\n" +
                "      </distributed-cache> ";
        String json = convert(xml, MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON);
        convert(json, MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML);
    }

    private String convert(String source, MediaType src, MediaType dst) {
        ParserRegistry parserRegistry = new ParserRegistry();
        Properties properties = new Properties();
        ConfigurationReader reader = ConfigurationReader.from(source)
                .withResolver(ConfigurationResourceResolvers.DEFAULT)
                .withType(src)
                .withProperties(properties)
                .withNamingStrategy(NamingStrategy.KEBAB_CASE).build();
        ConfigurationBuilderHolder holder = new ConfigurationBuilderHolder();
        parserRegistry.parse(reader, holder);
        Map.Entry<String, ConfigurationBuilder> entry = holder.getNamedConfigurationBuilders().entrySet().iterator().next();
        Configuration configuration = entry.getValue().build();
        StringBuilderWriter out = new StringBuilderWriter();
        try (ConfigurationWriter writer = ConfigurationWriter.to(out).withType(dst).clearTextSecrets(true).prettyPrint(true).build()) {
            parserRegistry.serialize(writer, entry.getKey(), configuration);
        }
        return out.toString();
    }
}
