package org.geotools.data.mongodb.complex.schemaless;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.opengis.feature.type.Name;

public class SchemalessFeatureTypeCache {

    private static SchemalessFeatureTypeCache schemalessCache;
    private final LoadingCache<Name, Optional<ModifiableComplexFeatureType>> cache;

    private SchemalessFeatureTypeCache() {
        cache =
                CacheBuilder.newBuilder()
                        .maximumSize(100)
                        .initialCapacity(1)
                        .expireAfterAccess(120, TimeUnit.MINUTES)
                        .build(
                                new CacheLoader<Name, Optional<ModifiableComplexFeatureType>>() {
                                    @Override
                                    public Optional<ModifiableComplexFeatureType> load(Name key) {
                                        return Optional.empty();
                                    }
                                });
    }

    public void updateCache(Name key, ModifiableComplexFeatureType featureType)
            throws ExecutionException {
        this.cache.asMap().put(key, Optional.of(featureType));
    }

    public ModifiableComplexFeatureType getFeatureType(Name key)
            throws ExecutionException, CloneNotSupportedException {
        ModifiableComplexFeatureType featureType = this.cache.get(key).orElseGet(() -> null);
        if (featureType != null) return (ModifiableComplexFeatureType) featureType.clone();
        return null;
    }

    public static SchemalessFeatureTypeCache getInstance() {
        if (schemalessCache == null) {
            synchronized (SchemalessFeatureTypeCache.class) {
                if (schemalessCache == null) {
                    schemalessCache = new SchemalessFeatureTypeCache();
                }
            }
        }
        return schemalessCache;
    }
}
