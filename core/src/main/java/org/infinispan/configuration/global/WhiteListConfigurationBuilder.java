package org.infinispan.configuration.global;

import static org.infinispan.configuration.global.WhiteListConfiguration.CLASSES;
import static org.infinispan.configuration.global.WhiteListConfiguration.REGEXPS;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.ClassWhiteList;
import org.infinispan.commons.configuration.attributes.AttributeSet;

/**
 * Configures the {@link org.infinispan.manager.EmbeddedCacheManager} {@link org.infinispan.commons.configuration.ClassWhiteList}.
 *
 * @author Ryan Emerson
 * @since 10.0
 */
public class WhiteListConfigurationBuilder implements Builder<WhiteListConfiguration> {

   private final AttributeSet attributes;
   private final Set<String> classes = new HashSet<>();
   private final List<String> regexps = new ArrayList<>();

   WhiteListConfigurationBuilder() {
      attributes = WhiteListConfiguration.attributeDefinitionSet();
   }

   /**
    * Helper method that allows for registration of a class to the {@link ClassWhiteList}.
    */
   public <T> WhiteListConfigurationBuilder addClass(String clazz) {
      this.classes.add(clazz);
      return this;
   }

   /**
    * Helper method that allows for registration of classes to the {@link ClassWhiteList}.
    */
   public <T> WhiteListConfigurationBuilder addClasses(Class... classes) {
      List<String> classNames = Arrays.stream(classes).map(Class::getName).collect(Collectors.toList());
      this.classes.addAll(classNames);
      return this;
   }


   /**
    * Helper method that allows for registration of a regexp to the {@link ClassWhiteList}.
    */
   public <T> WhiteListConfigurationBuilder addRegexp(String regex) {
      this.regexps.add(regex);
      return this;
   }

   /**
    * Helper method that allows for registration of regexps to the {@link ClassWhiteList}.
    */
   public <T> WhiteListConfigurationBuilder addRegexps(String... regexps) {
      this.regexps.addAll(Arrays.asList(regexps));
      return this;
   }

   @Override
   public void validate() {
      // No-op, no validation required
   }

   @Override
   public WhiteListConfiguration create() {
      if (!classes.isEmpty())
         attributes.attribute(CLASSES).set(classes);

      if (!regexps.isEmpty())
         attributes.attribute(REGEXPS).set(regexps);
      return new WhiteListConfiguration(attributes.protect());
   }

   @Override
   public Builder<?> read(WhiteListConfiguration template) {
      this.attributes.read(template.attributes());
      this.classes.addAll(template.getClasses());
      this.regexps.addAll(template.getRegexps());
      return this;
   }
}
