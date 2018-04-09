package org.qcri.rheem.rest.config;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by jlucas on 10/9/17.
 */
public enum BasicOperator {
    TEXT_FILE_SOURCE("org.qcri.rheem.basic.operators.TextFileSource"),
    REDUCE_BY_OPERATOR("org.qcri.rheem.basic.operators.ReduceByOperator"),
    REDUCE_OPERATOR("org.qcri.rheem.basic.operators.ReduceOperator"),
    MAP_OPERATOR("org.qcri.rheem.basic.operators.MapOperator"),
    FLAT_MAP_OPERATOR("org.qcri.rheem.basic.operators.FlatMapOperator"),
    LOOP_OPERATOR("org.qcri.rheem.basic.operators.LoopOperator"),
    JOIN_OPERATOR("org.qcri.rheem.basic.operators.JoinOperator"),
    GROUP_BY_OPERATOR("org.qcri.rheem.basic.operators.GroupByOperator"),
    TEXT_FILE_SINK("org.qcri.rheem.basic.operators.TextFileSink"),
    LOCAL_CALL_BACK_SINK("org.qcri.rheem.basic.operators.LocalCallbackSink");

    private String name;

    BasicOperator(String name) {
        this.name = name;
    }

    public String getName() {
        return this.name;
    }
}
