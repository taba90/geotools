package org.geotools.data.mongodb.complex.schemaless;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.gson.Gson;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.opengis.feature.type.Name;

public class SchemalessFeatureTypeCache {

    private static SchemalessFeatureTypeCache schemalessCache;
    private final LoadingCache<Name, Optional<String>> cache;
    private Gson gson;

    private SchemalessFeatureTypeCache() {
        cache =
                CacheBuilder.newBuilder()
                        .maximumSize(100)
                        .initialCapacity(1)
                        .expireAfterAccess(120, TimeUnit.MINUTES)
                        .build(
                                new CacheLoader<Name, Optional<String>>() {
                                    @Override
                                    public Optional<String> load(Name key) {
                                        return Optional.empty();
                                    }
                                });
        this.gson = new Gson();
    }

    public void updateCache(Name key, ModifiableComplexFeatureType featureType)
            throws ExecutionException {
        String json = gson.toJson(featureType);
        this.cache.asMap().put(key, Optional.of(json));
    }

    public ModifiableComplexFeatureType getFeatureType(Name key) throws ExecutionException {
        String featureType = this.cache.get(key).orElseGet(() -> null);
        ModifiableComplexFeatureType mcft =
                this.gson.fromJson(featureType, ModifiableComplexFeatureType.class);
        if (mcft != null) return mcft;
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
