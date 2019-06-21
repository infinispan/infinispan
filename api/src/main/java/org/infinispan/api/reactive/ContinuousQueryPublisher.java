package org.infinispan.api.reactive;

import org.reactivestreams.Publisher;

public interface ContinuousQueryPublisher<Id, Result> extends Publisher<KeyValueEntry<Id, Result>> {

}
