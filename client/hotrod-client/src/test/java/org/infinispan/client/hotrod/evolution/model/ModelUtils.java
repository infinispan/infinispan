package org.infinispan.client.hotrod.evolution.model;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.annotation.model.Model;

import java.util.function.Function;

public class ModelUtils {

    private static int ID_VERSION_OFFSET = 100000;

    public static Function<Integer, Model> createBaseModelEntity(int version) {
        return i -> {
            BaseModelEntity m = new BaseModelEntity();
            m.entityVersion = version;
            m.id = String.valueOf(i);
            m.name = "modelA # " + i;

            return m;
        };
    }

    public static Function<Integer, Model> createBaseModelWithNameFieldIndexedEntity(int version) {
        return i -> {
            BaseModelWithNameFieldIndexedEntity m = new BaseModelWithNameFieldIndexedEntity();
            m.entityVersion = version;
            m.id = String.valueOf(ID_VERSION_OFFSET + i);
            m.name = "modelB # " + i;

            return m;
        };
    }

    public static Function<Integer, Model> createBaseModelWithNameFieldAnalyzedEntity(int version) {
        return i -> {
            BaseModelWithNameFieldAnalyzedEntity m = new BaseModelWithNameFieldAnalyzedEntity();
            m.entityVersion = version;
            m.id = String.valueOf((2 * ID_VERSION_OFFSET) + i);
            m.nameAnalyzed = "modelC # " + i;

            return m;
        };
    }

    public static Function<Integer, Model> createBaseModelWithNameIndexedAndNameFieldEntity(int version) {
        return i -> {
            BaseModelWithNameIndexedAndNameFieldEntity m = new BaseModelWithNameIndexedAndNameFieldEntity();
            m.entityVersion = version;
            m.id = String.valueOf((3 * ID_VERSION_OFFSET) + i);
            m.name = "modelD # " + i;
            m.nameAnalyzed = "modelD # " + i;

            return m;
        };
    }

    public static Function<Integer, Model> createBaseModelWithNewIndexedFieldEntity(int version) {
        return i -> {
            BaseModelWithNewIndexedFieldEntity m = new BaseModelWithNewIndexedFieldEntity();
            m.entityVersion = version;
            m.id = String.valueOf((4 * ID_VERSION_OFFSET) + i);
            m.name = "modelE # " + i;
            m.newField = "cOoLNewField-" + i;

            return m;
        };
    }

    public static Function<Integer, Model> createBaseModelWithNameIndexedFieldEntity(int version) {
        return i -> {
            BaseModelWithNameIndexedFieldEntity m = new BaseModelWithNameIndexedFieldEntity();
            m.entityVersion = version;
            m.id = String.valueOf((5 * ID_VERSION_OFFSET) + i);
            m.nameAnalyzed = "modelF # " + i;

            return m;
        };
    }

    public static Function<Integer, Model> createBaseModelWithNameAnalyzedAndNameNonAnalyzedFieldEntity(int version) {
        return i -> {
            BaseModelWithNameAnalyzedAndNameNonAnalyzedFieldEntity m = new BaseModelWithNameAnalyzedAndNameNonAnalyzedFieldEntity();
            m.entityVersion = version;
            m.id = String.valueOf((6 * ID_VERSION_OFFSET) + i);
            m.nameAnalyzed = "modelG # " + i;
            m.nameNonAnalyzed = m.nameAnalyzed;

            return m;
        };
    }

    public static Function<Integer, Model> createBaseEntityWithNonAnalyzedNameFieldEntity(int version) {
        return i -> {
            BaseEntityWithNonAnalyzedNameFieldEntity m = new BaseEntityWithNonAnalyzedNameFieldEntity();
            m.entityVersion = version;
            m.id = String.valueOf((7 * ID_VERSION_OFFSET) + i);
            m.nameNonAnalyzed = "modelH # " + i;

            return m;
        };
    }

    public static Function<Integer, Model> createBaseModelWithNameFieldIndexedAndNameAnalyzedFieldEntity(int version) {
        return i -> {
            BaseModelWithNameFieldIndexedAndNameAnalyzedFieldEntity m = new BaseModelWithNameFieldIndexedAndNameAnalyzedFieldEntity();
            m.entityVersion = version;
            m.id = String.valueOf((7 * ID_VERSION_OFFSET) + i);
            m.nameAnalyzed = "modelI # " + i;
            m.name = "modelI # " + i;

            return m;
        };
    }

    public static Function<Integer, Model> createBaseModelWithNameFieldAnalyzedEntityOldAnnotations(int version) {
        return i -> {
            BaseModelWithNameFieldAnalyzedEntityOldAnnotations m = new BaseModelWithNameFieldAnalyzedEntityOldAnnotations();
            m.entityVersion = version;
            m.id = String.valueOf(i);
            m.nameAnalyzed = "modelJ # " + i;

            return m;
        };
    }

    public static void createModelEntities(RemoteCache<String, Model> cache, int number, Function<Integer, Model> modelProducer) {
        for (int i = 0; i < number; i++) {
            Model m = modelProducer.apply(i);
            cache.put(m.getId(), m);
        }
    }
}
