package org.infinispan.configuration.cache;

/**
 * Allows you to tune various unsafe or non-standard characteristics. Certain operations such as
 * Cache.put() that are supposed to return the previous value associated with the specified key
 * according to the java.util.Map contract will not fulfill this contract if unsafe toggle is turned
 * on. Use with care. See details at <a
 * href="https://docs.jboss.org/author/display/ISPN/Technical+FAQs">Technical FAQ</a>
 * 
 * @author pmuir
 * 
 */
public class UnsafeConfigurationBuilder extends AbstractConfigurationChildBuilder<UnsafeConfiguration> {

   private boolean unreliableReturnValues = false;

   protected UnsafeConfigurationBuilder(ConfigurationBuilder builder) {
      super(builder);
   }

   public UnsafeConfigurationBuilder unreliableReturnValues(boolean b) {
      this.unreliableReturnValues = b;
      return this;
   }

   @Override
   void validate() {
      // TODO Auto-generated method stub

   }

   @Override
   UnsafeConfiguration create() {
      return new UnsafeConfiguration(unreliableReturnValues);
   }

   @Override
   public UnsafeConfigurationBuilder read(UnsafeConfiguration template) {
      this.unreliableReturnValues = template.unreliableReturnValues();

      return this;
   }

}
