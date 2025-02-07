package org.apache.flink.table.planner.plan.nodes.exec.serde;

import org.apache.flink.util.jackson.JacksonMapperFactory;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.InjectableValues;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectReader;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.smile.SmileFactory;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.smile.SmileGenerator;

import static org.apache.flink.table.planner.plan.nodes.exec.serde.JsonSerdeUtil.createFlinkTableJacksonModule;

public class SmileSerdeUtil {
    private static final SmileFactory smileFactory = new SmileFactory();
    private static final ObjectMapper smileMapper =
            JacksonMapperFactory.createObjectMapper(smileFactory)
                    .disable(SerializationFeature.FAIL_ON_EMPTY_BEANS)
                    .registerModule(createFlinkTableJacksonModule());

    public static ObjectWriter createObjectWriter(SerdeContext serdeContext) {
        return smileMapper
                .writer()
                .withAttribute(SerdeContext.SERDE_CONTEXT_KEY, serdeContext)
                .with(SmileGenerator.Feature.CHECK_SHARED_STRING_VALUES);
    }

    public static ObjectReader createObjectReader(SerdeContext serdeContext) {
        return smileMapper
                .reader()
                .withAttribute(SerdeContext.SERDE_CONTEXT_KEY, serdeContext)
                .with(SmileGenerator.Feature.CHECK_SHARED_STRING_VALUES)
                .with(defaultInjectedValues());
    }

    private static InjectableValues defaultInjectedValues() {
        return new InjectableValues.Std().addValue("isDeserialize", true);
    }
}
