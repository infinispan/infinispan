package ${package};

import java.util.EnumSet;
import java.util.Set;
import java.util.concurrent.CompletionStage;

import org.infinispan.commons.configuration.ConfiguredBy;
import org.infinispan.configuration.cache.CustomStoreConfiguration;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.persistence.spi.NonBlockingStore;
import org.kohsuke.MetaInfServices;

@MetaInfServices
@ConfiguredBy(CustomStoreConfiguration.class)
/**
 * This is an example {@link NonBlockingStore} implementation that provides basic
 */
public class CustomStore<K, V> implements NonBlockingStore<K, V> {

   /*
    * Mandatory method implementations.
    */

   @Override
   public CompletionStage<Void> start(InitializationContext ctx) {
      /**
       * This method will be invoked by the PersistenceManager during initialization. See the {@link InitializationContext}
       * JavaDocs for details of the provided components.
       * <p>
       * This method is guaranteed not to be invoked concurrently with other operations. This means other methods are
       * not invoked on this store until after the returned Stage completes.
       */
      return null;  // TODO: Customise this generated block
   }

   @Override
   public CompletionStage<Void> stop() {
      return null;  // TODO: Customise this generated block
   }

   @Override
   public CompletionStage<MarshallableEntry<K, V>> load(int segment, Object key) {
      /**
       * If the store provides the {@link NonBlockingStore.Characteristic.WRITE_ONLY} characteristic via a
       * {@link #characteristics()} implementation, then this can simply return null.
       */
      return null;  // TODO: Customise this generated block
   }

   @Override
   public CompletionStage<Void> write(int segment, MarshallableEntry<? extends K, ? extends V> entry) {
      /**
       * If the store provides the {@link NonBlockingStore.Characteristic.READ_ONLY} characteristic via a
       * {@link #characteristics()} implementation, then this can simply return null.
       */
      return null;  // TODO: Customise this generated block
   }

   @Override
   public CompletionStage<Boolean> delete(int segment, Object key) {
      /**
       * If the store provides the {@link NonBlockingStore.Characteristic.READ_ONLY} characteristic via a
       * {@link #characteristics()} implementation, then this can simply return null.
       */
      return null;  // TODO: Customise this generated block
   }

   @Override
   public CompletionStage<Void> clear() {
      /**
       * If the store provides the {@link NonBlockingStore.Characteristic.READ_ONLY} characteristic via a
       * {@link #characteristics()} implementation, then this can simply return null.
       */
      return null;  // TODO: Customise this generated block
   }

   @Override
   public Set<Characteristic> characteristics() {
      /**
       * If your store implementation wants to support one or more of the {@link NonBlockingStore.Characteristic}s,
       * then it's necessary to update this method to return a {@link Set} of the supported characteristics.
       * <p>
       * For example:
       * If your store is read-only, the following should be returned:
       * <code>return EnumSet.of(Characteristic.READ_ONLY);</code>
       * <p>
       * Conversely, if your store only accepts writes and is shareable, the following is required:
       * <code>return EnumSet.of(Characteristic.WRITE_ONLY, Characteristic.SHAREABLE);</code>
       */
      return EnumSet.noneOf(Characteristic.class);
   }
}
