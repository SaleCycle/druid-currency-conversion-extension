package com.salecycle.druid;

import io.druid.query.aggregation.CountAggregator;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

public class CurrencyPostAggregatorTest {

    @Test
    public void computeCurrency() {
        String aggName = "total";
        String curName = "currency";
        CountAggregator agg = new CountAggregator();
        agg.aggregate();
        agg.aggregate();
        agg.aggregate();
        Map<String, Object> metricValues = new HashMap<>();
        metricValues.put(aggName, agg.get());
        metricValues.put(curName, "USD");
        Map<String, Double> conversionValue = spy(new HashMap<>());
        conversionValue.put("GBP", 1.0);
        conversionValue.put("USD", 2.0);
        PostAggregator field = new FieldAccessPostAggregator("total", "total");
        PostAggregator currencyField = new FieldAccessPostAggregator("currency", "currency");
        CurrencyPostAggregator aggregator = new CurrencyPostAggregator("converted", conversionValue, field, currencyField);

        Object value = aggregator.compute(metricValues);
        verify(conversionValue).get("USD");
        assertThat(value, is(equalTo(6.0)));
    }
    @Test
    public void computeCurrencyFromZeroValue() {
        String aggName = "total";
        String curName = "currency";
        CountAggregator agg = new CountAggregator();
        Map<String, Object> metricValues = new HashMap<>();
        metricValues.put(aggName, agg.get());
        metricValues.put(curName, "USD");
        Map<String, Double> conversionValue = spy(new HashMap<>());
        conversionValue.put("GBP", 1.0);
        conversionValue.put("USD", 2.0);
        PostAggregator field = new FieldAccessPostAggregator("total", "total");
        PostAggregator currencyField = new FieldAccessPostAggregator("currency", "currency");
        CurrencyPostAggregator aggregator = new CurrencyPostAggregator("converted", conversionValue, field, currencyField);

        Object value = aggregator.compute(metricValues);
        verify(conversionValue, never()).get(any());
        assertThat(value, is(equalTo(0.0)));
    }
    @Test
    public void computeDefault() {
        String aggName = "total";
        String curName = "currency";
        CountAggregator agg = new CountAggregator();
        agg.aggregate();
        agg.aggregate();
        agg.aggregate();
        Map<String, Object> metricValues = new HashMap<>();
        metricValues.put(aggName, agg.get());
        metricValues.put(curName, "GBP");
        Map<String, Double> conversionValue = spy(new HashMap<>());
        conversionValue.put("GBP", 1.0);
        conversionValue.put("USD", 2.0);
        PostAggregator field = new FieldAccessPostAggregator("total", "total");
        PostAggregator currencyField = new FieldAccessPostAggregator("currency", "currency");
        CurrencyPostAggregator aggregator = new CurrencyPostAggregator("converted", conversionValue, field, currencyField);

        Object value = aggregator.compute(metricValues);
        verify(conversionValue).get("GBP");
        assertThat(value, is(equalTo(3.0)));
    }
    @Test
    public void computeCurrencyNotInMap() {
        String aggName = "total";
        String curName = "currency";
        CountAggregator agg = new CountAggregator();
        agg.aggregate();
        agg.aggregate();
        agg.aggregate();
        Map<String, Object> metricValues = new HashMap<>();
        metricValues.put(aggName, agg.get());
        metricValues.put(curName, "EUR");
        Map<String, Double> conversionValue = spy(new HashMap<>());
        conversionValue.put("GBP", 1.0);
        conversionValue.put("USD", 2.0);
        PostAggregator field = new FieldAccessPostAggregator("total", "total");
        PostAggregator currencyField = new FieldAccessPostAggregator("currency", "currency");
        CurrencyPostAggregator aggregator = new CurrencyPostAggregator("converted", conversionValue, field, currencyField);

        Object value = aggregator.compute(metricValues);
        verify(conversionValue).get("EUR");
        assertThat(value, is(equalTo(0.0)));
    }
}