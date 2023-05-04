package org.apache.flink.table.gateway.rest.message.util;

import org.apache.flink.runtime.rest.messages.ResponseBody;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

/** {@link ResponseBody} for getting current catalog. */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class GetCurrentDatabaseResponseBody implements ResponseBody {

    private static final String FIELD_CURRENT_DATABASE = "currentDatabase";

    @JsonProperty(FIELD_CURRENT_DATABASE)
    private final String currentDatabase;

    @JsonCreator
    public GetCurrentDatabaseResponseBody(
            @JsonProperty(FIELD_CURRENT_DATABASE) String currentDatabase) {
        this.currentDatabase = currentDatabase;
    }

    public String getCurrentDatabase() {
        return currentDatabase;
    }
}
