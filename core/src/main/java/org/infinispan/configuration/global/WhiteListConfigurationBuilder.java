package org.infinispan.configuration.global;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.ClassAllowList;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.AttributeSet;

/**
 * @deprecated since 12.0. Use {@link AllowListConfigurationBuilder} through @{@link SerializationConfigurationBuilder#allowList()}. Will be removed in 14.0.
 */
@Deprecated
public class WhiteListConfigurationBuilder implements Builder<WhiteListConfiguration> {
   private final AllowListConfigurationBuilder delegate;

   WhiteListConfigurationBuilder(AllowListConfigurationBuilder delegate) {
      this.delegate = delegate;
   }

   @Override
   public AttributeSet attributes() {
      return delegate.attributes();
   }

   /**
    * Helper method that allows for registration of a class to the {@link ClassAllowList}.
    */
   public <T> WhiteListConfigurationBuilder addClass(String clazz) {
      delegate.addClass(clazz);
      return this;
   }

   /**
    * Helper method that allows for registration of classes to the {@link ClassAllowList}.
    */
   public <T> WhiteListConfigurationBuilder addClasses(Class... classes) {
      delegate.addClasses(classes);
      return this;
   }


   /**
    * Helper method that allows for registration of a regexp to the {@link ClassAllowList}.
    */
   public <T> WhiteListConfigurationBuilder addRegexp(String regex) {
      delegate.addRegexp(regex);
      return this;
   }

   /**
    * Helper method that allows for registration of regexps to the {@link ClassAllowList}.
    */
   public <T> WhiteListConfigurationBuilder addRegexps(String... regexps) {
      delegate.addRegexps(regexps);
      return this;
   }

   @Override
   public void validate() {
      // No-op, no validation required
   }

   @Override
   public WhiteListConfiguration create() {
      throw new UnsupportedOperationException();
   }

   @Override
   public Builder<?> read(WhiteListConfiguration template, Combine combine) {
      delegate.read(template.delegate, combine);
      return this;
   }
}
