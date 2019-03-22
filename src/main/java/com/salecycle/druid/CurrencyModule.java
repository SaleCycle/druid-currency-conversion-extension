package com.salecycle.druid;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.inject.Binder;
import org.apache.druid.initialization.DruidModule;

import java.util.Collections;
import java.util.List;

public class CurrencyModule implements DruidModule {
    @Override
    public List<? extends Module> getJacksonModules() {
        return Collections.singletonList(
                new SimpleModule("CurrencyModule").registerSubtypes(
                        new NamedType(CurrencyPostAggregator.class, "currencyConversion")
                )
        );
    }

    @Override
    public void configure(Binder binder) {

    }
}
