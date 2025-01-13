package org.infinispan.multimap.impl;

import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.ProtoSchema;
import org.infinispan.protostream.annotations.ProtoSyntax;

@ProtoSchema(
      allowNullFields = true,
      dependsOn = {
            org.infinispan.commons.GlobalContextInitializer.class,
            org.infinispan.marshall.protostream.impl.GlobalContextInitializer.class,
            org.infinispan.multimap.impl.PersistenceContextInitializer.class
      },
      includeClasses = {
            org.infinispan.multimap.impl.SortedSetBucket.AggregateFunction.class,
            org.infinispan.multimap.impl.function.hmap.HashMapKeySetFunction.class,
            org.infinispan.multimap.impl.function.hmap.HashMapPutFunction.class,
            org.infinispan.multimap.impl.function.hmap.HashMapRemoveFunction.class,
            org.infinispan.multimap.impl.function.hmap.HashMapReplaceFunction.class,
            org.infinispan.multimap.impl.function.hmap.HashMapValuesFunction.class,
            org.infinispan.multimap.impl.function.list.IndexFunction.class,
            org.infinispan.multimap.impl.function.list.IndexOfFunction.class,
            org.infinispan.multimap.impl.function.list.InsertFunction.class,
            org.infinispan.multimap.impl.function.list.OfferFunction.class,
            org.infinispan.multimap.impl.function.list.PollFunction.class,
            org.infinispan.multimap.impl.function.list.RemoveCountFunction.class,
            org.infinispan.multimap.impl.function.list.ReplaceListFunction.class,
            org.infinispan.multimap.impl.function.list.RotateFunction.class,
            org.infinispan.multimap.impl.function.list.SetFunction.class,
            org.infinispan.multimap.impl.function.list.SubListFunction.class,
            org.infinispan.multimap.impl.function.list.TrimFunction.class,
            org.infinispan.multimap.impl.function.multimap.ContainsFunction.class,
            org.infinispan.multimap.impl.function.multimap.GetFunction.class,
            org.infinispan.multimap.impl.function.multimap.PutFunction.class,
            org.infinispan.multimap.impl.function.multimap.RemoveFunction.class,
            org.infinispan.multimap.impl.function.set.SAddFunction.class,
            org.infinispan.multimap.impl.function.set.SGetFunction.class,
            org.infinispan.multimap.impl.function.set.SMIsMember.class,
            org.infinispan.multimap.impl.function.set.SPopFunction.class,
            org.infinispan.multimap.impl.function.set.SRemoveFunction.class,
            org.infinispan.multimap.impl.function.set.SSetFunction.class,
            org.infinispan.multimap.impl.function.sortedset.AddManyFunction.class,
            org.infinispan.multimap.impl.function.sortedset.CountFunction.class,
            org.infinispan.multimap.impl.function.sortedset.IncrFunction.class,
            org.infinispan.multimap.impl.function.sortedset.IndexOfSortedSetFunction.class,
            org.infinispan.multimap.impl.function.sortedset.PopFunction.class,
            org.infinispan.multimap.impl.function.sortedset.RemoveManyFunction.class,
            org.infinispan.multimap.impl.function.sortedset.ScoreFunction.class,
            org.infinispan.multimap.impl.function.sortedset.SortedSetAggregateFunction.class,
            org.infinispan.multimap.impl.function.sortedset.SortedSetAggregateFunction.AggregateType.class,
            org.infinispan.multimap.impl.function.sortedset.SortedSetOperationType.class,
            org.infinispan.multimap.impl.function.sortedset.SortedSetRandomFunction.class,
            org.infinispan.multimap.impl.function.sortedset.SubsetFunction.class
      },
      schemaFileName = "global.multimap.proto",
      schemaFilePath = "proto/generated",
      schemaPackageName = "org.infinispan.global.multimap",
      service = false,
      syntax = ProtoSyntax.PROTO3
)
interface GlobalContextInitializer extends SerializationContextInitializer {
}
