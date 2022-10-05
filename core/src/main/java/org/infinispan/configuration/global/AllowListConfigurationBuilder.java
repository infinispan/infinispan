package org.infinispan.configuration.global;

import static org.infinispan.configuration.global.AllowListConfiguration.CLASSES;
import static org.infinispan.configuration.global.AllowListConfiguration.REGEXPS;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.ClassAllowList;
import org.infinispan.commons.configuration.attributes.AttributeSet;

/**
 * Configures the {@link org.infinispan.manager.EmbeddedCacheManager} {@link ClassAllowList}.
 *
 * @author Ryan Emerson
 * @since 10.0
 */
public class AllowListConfigurationBuilder implements Builder<AllowListConfiguration> {

   private final AttributeSet attributes;
   private final Set<String> classes = new HashSet<>();
   private final List<String> regexps = new ArrayList<>();
   private final GlobalConfigurationBuilder globalBuilder;

   AllowListConfigurationBuilder(GlobalConfigurationBuilder globalBuilder) {
      this.globalBuilder = globalBuilder;
      attributes = AllowListConfiguration.attributeDefinitionSet();
   }

   /**
    * Helper method that allows for registration of a class to the {@link ClassAllowList}.
    */
   public <T> AllowListConfigurationBuilder addClass(String clazz) {
      this.classes.add(clazz);
      return this;
   }

   /**
    * Helper method that allows for registration of classes to the {@link ClassAllowList}.
    */
   public <T> AllowListConfigurationBuilder addClasses(String... classes) {
      List<String> classNames = Arrays.asList(classes);
      this.classes.addAll(classNames);
      return this;
   }

   /**
    * Helper method that allows for registration of classes to the {@link ClassAllowList}.
    */
   public <T> AllowListConfigurationBuilder addClasses(Class... classes) {
      List<String> classNames = Arrays.stream(classes).map(Class::getName).collect(Collectors.toList());
      this.classes.addAll(classNames);
      return this;
   }


   /**
    * Helper method that allows for registration of a regexp to the {@link ClassAllowList}.
    */
   public <T> AllowListConfigurationBuilder addRegexp(String regex) {
      this.regexps.add(regex);
      return this;
   }

   /**
    * Helper method that allows for registration of regexps to the {@link ClassAllowList}.
    */
   public <T> AllowListConfigurationBuilder addRegexps(String... regexps) {
      this.regexps.addAll(Arrays.asList(regexps));
      return this;
   }

   @Override
   public void validate() {
      // No-op, no validation required
   }

   @Override
   public AllowListConfiguration create() {
      if (!classes.isEmpty())
         attributes.attribute(CLASSES).set(classes);

      if (!regexps.isEmpty())
         attributes.attribute(REGEXPS).set(regexps);
      return new AllowListConfiguration(attributes.protect(), globalBuilder.getClassLoader());
   }

   @Override
   public Builder<?> read(AllowListConfiguration template) {
      this.attributes.read(template.attributes());
      this.classes.addAll(template.getClasses());
      this.regexps.addAll(template.getRegexps());
      return this;
   }
}
