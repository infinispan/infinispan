package org.infinispan.tools.store.migrator.marshaller.common;

import java.io.IOException;
import java.io.ObjectInput;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.metadata.EmbeddedMetadata;

public class EmbeddedMetadataExternalizer extends AbstractMigratorExternalizer<EmbeddedMetadata> {

   private static final int IMMORTAL = 0;
   private static final int EXPIRABLE = 1;
   private static final int LIFESPAN_EXPIRABLE = 2;
   private static final int MAXIDLE_EXPIRABLE = 3;
   private final Map<Class<?>, Integer> numbers = new HashMap<>(4);

   public EmbeddedMetadataExternalizer() {
      super(
            Set.of(
                  EmbeddedMetadata.class, EmbeddedMetadata.EmbeddedExpirableMetadata.class,
                  EmbeddedMetadata.EmbeddedLifespanExpirableMetadata.class, EmbeddedMetadata.EmbeddedMaxIdleExpirableMetadata.class
            ),
            Ids.EMBEDDED_METADATA
      );
      numbers.put(EmbeddedMetadata.class, IMMORTAL);
      numbers.put(EmbeddedMetadata.EmbeddedExpirableMetadata.class, EXPIRABLE);
      numbers.put(EmbeddedMetadata.EmbeddedLifespanExpirableMetadata.class, LIFESPAN_EXPIRABLE);
      numbers.put(EmbeddedMetadata.EmbeddedMaxIdleExpirableMetadata.class, MAXIDLE_EXPIRABLE);
   }

   @Override
   public EmbeddedMetadata readObject(ObjectInput input) throws IOException, ClassNotFoundException {
      int number = input.readByte();
      EmbeddedMetadata.Builder builder = new EmbeddedMetadata.Builder();
      switch (number) {
         case IMMORTAL:
            builder.version((EntryVersion) input.readObject());
            break;
         case EXPIRABLE:
            builder.lifespan(toMillis(input.readLong(), TimeUnit.MILLISECONDS))
                  .maxIdle(toMillis(input.readLong(), TimeUnit.MILLISECONDS))
                  .version((EntryVersion) input.readObject());
            break;
         case LIFESPAN_EXPIRABLE:
            builder.lifespan(toMillis(input.readLong(), TimeUnit.MILLISECONDS))
                  .version((EntryVersion) input.readObject());
            break;
         case MAXIDLE_EXPIRABLE:
            builder.maxIdle(toMillis(input.readLong(), TimeUnit.MILLISECONDS))
                  .version((EntryVersion) input.readObject());
         default:
            throw new IllegalStateException("Unknown metadata type " + number);
      }
      return (EmbeddedMetadata) builder.build();
   }

   private static long toMillis(long duration, TimeUnit timeUnit) {
      return duration < 0 ? -1 : timeUnit.toMillis(duration);
   }
}
