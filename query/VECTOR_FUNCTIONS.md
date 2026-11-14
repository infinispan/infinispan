# Built-in Vector Functions — Design Sketch

Status: proposal
Scope: `query/` module (`QueryFunctionRegistry`)
Dependency: `jdk.incubator.vector` (already enabled in the server container for Lucene)

## Motivation

Embedding fields (`float[]` / `double[]`, typically 768–3072 dims) are increasingly
stored in Infinispan caches. A small set of scalar-returning vector operations is
useful in `UPDATE` statements for normalisation, validation, and derived-metadata
columns without requiring users to deploy a UDF JAR.

## Proposed built-ins

| Function | Signature | Returns | Description |
|----------|-----------|---------|-------------|
| `vectorDim`  | `(float[] v)` | `int`     | Number of dimensions |
| `vectorNorm` | `(float[] v)` | `double`  | L2 (Euclidean) norm |
| `vectorDot`  | `(float[] a, float[] b)` | `double` | Dot product (second arg typically a bound parameter) |

All functions return `null` when any argument is `null` (consistent with other built-ins).

## Implementation sketch

```java
import jdk.incubator.vector.FloatVector;
import jdk.incubator.vector.VectorSpecies;

import static java.lang.Math.fma;
import static java.lang.Math.sqrt;

final class VectorFunctions {

    private static final VectorSpecies<Float> SPECIES = FloatVector.SPECIES_PREFERRED;

    private VectorFunctions() {}

    static int dim(float[] v) {
        return v.length;
    }

    static double norm(float[] v) {
        float sumSq = 0f;
        int i = 0;
        int upper = v.length - (v.length % SPECIES.length());
        for (; i < upper; i += SPECIES.length()) {
            FloatVector vec = FloatVector.fromArray(SPECIES, v, i);
            sumSq += (float) vec.mul(vec).reduceSum();
        }
        for (; i < v.length; i++) {
            sumSq = fma(v[i], v[i], sumSq);
        }
        return sqrt(sumSq);
    }

    static double dot(float[] a, float[] b) {
        if (a.length != b.length) {
            throw new IllegalArgumentException(
                "vectorDot: dimension mismatch (" + a.length + " vs " + b.length + ")");
        }
        float sum = 0f;
        int i = 0;
        int upper = a.length - (a.length % SPECIES.length());
        for (; i < upper; i += SPECIES.length()) {
            FloatVector va = FloatVector.fromArray(SPECIES, a, i);
            FloatVector vb = FloatVector.fromArray(SPECIES, b, i);
            sum += (float) va.mul(vb).reduceSum();
        }
        for (; i < a.length; i++) {
            sum = fma(a[i], b[i], sum);
        }
        return sum;
    }
}
```

### Registration in `QueryFunctionRegistry.standard()`

```java
register("vectorDim", args -> {
    float[] v = requireFloatArray(args, 0, "vectorDim");
    return VectorFunctions.dim(v);
});

register("vectorNorm", args -> {
    float[] v = requireFloatArray(args, 0, "vectorNorm");
    return VectorFunctions.norm(v);
});

register("vectorDot", args -> {
    float[] a = requireFloatArray(args, 0, "vectorDot");
    float[] b = requireFloatArray(args, 1, "vectorDot");
    return VectorFunctions.dot(a, b);
});
```

### Type coercion helper

```java
private static float[] requireFloatArray(List<Object> args, int idx, String fn) {
    Object o = args.get(idx);
    if (o == null) return null;
    if (o instanceof float[] fa) return fa;
    if (o instanceof double[] da) {
        float[] fa = new float[da.length];
        for (int i = 0; i < da.length; i++) fa[i] = (float) da[i];
        return fa;
    }
    throw new IllegalArgumentException(fn + " expects a float[] or double[] but got " + o.getClass().getSimpleName());
}
```

## Examples

```sql
-- Store the norm alongside the embedding
UPDATE FROM Document SET embNorm = vectorNorm(embedding) WHERE embedding IS NOT NULL

-- Validate dimensionality before a bulk re-index
UPDATE FROM Document SET invalid = true
WHERE vectorDim(embedding) != 1536

-- Flag documents highly similar to a known-spam centroid
UPDATE FROM Document SET spamScore = vectorDot(embedding, :centroid)
WHERE vectorDot(embedding, :centroid) > 0.92
```

## Open questions

1. **`float[]` vs `double[]`** — Lucene / most embedding models use `float32`.
   Should we also register `double[]`-native variants (`vectorNormD`, …) or rely
   on the coercion helper? Leaning toward coercion only (simpler surface).

2. **`vectorNormalize` (vector-returning)** — deferred. Writing a `float[]` back
   through `ProtobufFieldAccessor` into a repeated field is possible but adds
   complexity; revisit if demand appears.

3. **Module isolation** — `VectorFunctions` lives in the `query` module which is
   also used embeddable (no server). The incubator module is not available in a
   plain embeddable JVM unless the user adds it. Options:
   - (a) Accept it: the functions are registered unconditionally; calling them
     without the module throws `NoSuchModuleException` at runtime.
   - (b) Guard registration: check `Class.forName("jdk.incubator.vector.FloatVector")`
     and skip registration if absent.
   - (c) Put `VectorFunctions` in `server/runtime` and register via the same
     `ServiceLoader` path as UDFs.
   
   Leaning toward **(b)** — graceful degradation, zero cost when unused.

## Testing

- Unit tests in `UpdateValueEvaluatorTest` (or a dedicated `VectorFunctionTest`):
  - known-vector norm / dot against hand-computed values
  - dimension-mismatch exception
  - `null` propagation
  - `double[]` coercion path
  - tail-loop correctness (vectors whose length is not a multiple of `SPECIES.length()`)
