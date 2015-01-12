/*
 * Copyright 2014 (c) Raythos Interactive Ltd.  http://www.raythos.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.raythos.sentilexo.storm.pmml;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.dmg.pmml.FieldName;
//import org.dmg.pmml.FieldValue;
import org.jpmml.evaluator.Evaluator;
import org.jpmml.evaluator.FieldValue;
import org.jpmml.evaluator.EvaluatorUtil;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

/**
 *
 * @author yanni
 */
public class PmmlModelEvaluationFunction extends BaseFunction {

    private Fields outFields;
    private Evaluator evaluator;

    public Fields getFields() {
        return outFields;
    }

    public void setFields(Fields fields) {
        this.outFields = fields;
    }

    public Evaluator getEvaluator() {
        return evaluator;
    }

    public void setEvaluator(Evaluator evaluator) {
        this.evaluator = evaluator;
    }

    void updateFields() {
        Evaluator evaluator = getEvaluator();
        List<String> fields = new ArrayList<String>();
        List<FieldName> targetFields = evaluator.getTargetFields();
        for (FieldName targetField : targetFields) {
            fields.add(targetField.getValue());
        }
        List<FieldName> outputFields = evaluator.getOutputFields();
        for (FieldName outputField : outputFields) {
            fields.add(outputField.getValue());
        }
        outFields = new Fields(fields);
    }

    @Override
    public void execute(TridentTuple tuple, TridentCollector tc) {
        Evaluator evaluator = getEvaluator();
        Map<FieldName, FieldValue> arguments = new LinkedHashMap<>();
        List<FieldName> activeFields = evaluator.getActiveFields();
        for (FieldName activeField : activeFields) {
            String fieldName = activeField.getValue();
            FieldValue value = EvaluatorUtil.prepare(evaluator, activeField, tuple.getValueByField(fieldName));
            arguments.put(activeField, value);
        }
        Map<FieldName, ?> result = evaluator.evaluate(arguments);
        Values values = new Values();
        List<FieldName> targetFields = evaluator.getTargetFields();
        for (FieldName targetField : targetFields) {
            values.add(EvaluatorUtil.decode(result.get(targetField)));
        }
        List<FieldName> outputFields = evaluator.getOutputFields();
        for (FieldName outputField : outputFields) {
            values.add(result.get(outputField));
        }
        tc.emit(values);
    }

}
