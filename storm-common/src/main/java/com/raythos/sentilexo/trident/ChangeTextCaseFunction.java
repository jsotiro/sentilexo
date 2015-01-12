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
package com.raythos.sentilexo.trident;

import backtype.storm.tuple.Values;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.Filter;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

/**
 *
 * @author yanni
 */
@SuppressWarnings({"serial", "rawtypes"})
public class ChangeTextCaseFunction extends BaseFunction {

    protected static Logger log = LoggerFactory.getLogger(ChangeTextCaseFunction.class);

    public static final int TO_LOWER_CASE = -1;
    public static final int NO_CHANGE = 0;
    public static final int TO_UPPER_CASE = 1;

    private int changeCaseType = NO_CHANGE;

    public int getChangeCaseType() {
        return changeCaseType;
    }

    public void setChangeCaseType(int changeCaseType) {
        this.changeCaseType = changeCaseType;
    }

    public ChangeTextCaseFunction(int changeCaseType) {
        super();
        this.changeCaseType = changeCaseType;
    }

    @Override
    public void prepare(Map conf, TridentOperationContext context) {

    }

    @Override
    public void cleanup() {
    }

    @Override
    public void execute(TridentTuple tuple, TridentCollector tc) {
        String text = tuple.getString(0);

        text = (changeCaseType == TO_LOWER_CASE ? text.toLowerCase()
                : (changeCaseType == TO_UPPER_CASE ? text.toUpperCase()
                        : text));

        tc.emit(new Values(text));
    }

}
