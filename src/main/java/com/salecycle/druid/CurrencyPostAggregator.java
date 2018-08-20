package com.salecycle.druid;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import io.druid.java.util.common.guava.Comparators;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.cache.CacheKeyBuilder;
import org.apache.commons.lang.StringUtils;

import java.util.Comparator;
import java.util.Map;
import java.util.Set;

/**
 * CurrencyPostAggregator converts all currency outputs to a target currency
 *
 * post aggregator example:
 * <pre>{@code
 * {
 *   "type": "currencyConversion",
 *   "name": "convertedValue",
 *   "currency": "GBP",
 *   "field": { "type" : "fieldAccess", "name": "total", "fieldName" : "total" },
 *   "currencyField": { "type" : "fieldAccess", "name": "currency", "fieldName" : "currency" },
 *   "conversions": {"USD": 1.6, "GBP": 1.0, "EUR": 0.89}
 * }
 * }</pre>
 */
public class CurrencyPostAggregator implements PostAggregator {
    private final byte CURRENCY = 1;
    private static final Comparator DEFAULT_COMPARATOR = Comparators.naturalNullsFirst();
    private final ObjectMapper mapper = new ObjectMapper();
    private String name;
    private String currency;
    private Map<String, Double> conversions;
    private final PostAggregator field;
    private final PostAggregator currencyField;

    @JsonCreator
    public CurrencyPostAggregator(
      @JsonProperty("name") String name,
      @JsonProperty("currency") String currency,
      @JsonProperty("conversions") Map<String, Double> conversions,
      @JsonProperty("field") PostAggregator field,
      @JsonProperty("currencyField") PostAggregator currencyField
    ){
        Preconditions.checkArgument(StringUtils.isNotBlank(currency), "currency cannot be null");
        Preconditions.checkArgument(conversions != null && conversions.size() > 0, "must have a conversion set");
        Preconditions.checkArgument(!conversions.containsValue(null), "no conversion value can be null");
        Preconditions.checkArgument(field != null, "field cannot be null");
        Preconditions.checkArgument(currencyField != null, "currency field cannot be null");

        this.name = name;
        this.currency = currency;
        this.conversions = conversions;
        this.field = field;
        this.currencyField = currencyField;
    }

    @Override
    public Set<String> getDependentFields() {
        Set<String> dependentFields = Sets.newHashSet();
        dependentFields.addAll(field.getDependentFields());
        dependentFields.addAll(currencyField.getDependentFields());
        return dependentFields;
    }

    @Override
    public Comparator getComparator() {
        return DEFAULT_COMPARATOR;
    }

    @Override
    public Object compute(Map<String, Object> values) {
        double value = ((Number) field.compute(values)).doubleValue();
        String fieldCurrency = currencyField.compute(values).toString();
        return convertCurrency(fieldCurrency, value);
    }

    @Override
    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty("currency")
    public String getCurrency() {
        return currency;
    }

    @JsonProperty("conversions")
    public Map<String, Double> getConversions() {
        return conversions;
    }
    @JsonProperty("field")
    public PostAggregator getField() {
        return field;
    }
    @JsonProperty("currencyField")
    public PostAggregator getCurrencyField() {
        return currencyField;
    }

    @Override
    public CurrencyPostAggregator decorate(Map<String, AggregatorFactory> aggregators) {
        return this;
    }

    @Override
    public byte[] getCacheKey() {
        CacheKeyBuilder cacheKeyBuilder = new CacheKeyBuilder(CURRENCY)
                .appendString(currency)
                .appendCacheable(field)
                .appendCacheable(currencyField);
        // append conversions
        try {
            cacheKeyBuilder.appendString(mapper.writeValueAsString(conversions));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return cacheKeyBuilder.build();
    }
    @Override
    public String toString()
    {
        return "CurrencyPostAggregator{" +
                "name='" + name + '\'' +
                ", currency='" + currency + '\'' +
                ", field=" + field +
                ", currencyField=" + currencyField +
                '}';
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        CurrencyPostAggregator that = (CurrencyPostAggregator) o;

        if (!conversions.equals(that.conversions)) {
            return false;
        }
        if (!field.equals(that.field)) {
            return false;
        }
        if (!currencyField.equals(that.currencyField)) {
            return false;
        }
        if (name != null ? !name.equals(that.name) : that.name != null) {
            return false;
        }
        if (!currency.equals(that.currency)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + field.hashCode();
        result = 31 * result + currencyField.hashCode();
        result = 31 * result + currency.hashCode();
        result = 31 * result + conversions.hashCode();
        return result;
    }

    private double convertCurrency(String fromCurrency, double value) {
        if (fromCurrency.equals(currency)) {
            return value;
        }
        double multiplier = conversions.get(fromCurrency);
        return value * multiplier;
    }
}
