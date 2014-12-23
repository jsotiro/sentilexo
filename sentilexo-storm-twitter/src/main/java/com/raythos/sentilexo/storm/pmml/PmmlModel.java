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

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import javax.xml.bind.JAXBException;
import javax.xml.transform.Source;
import org.dmg.pmml.PMML;
import org.jpmml.evaluator.ModelEvaluator;
import org.jpmml.evaluator.ModelEvaluatorFactory;
import org.jpmml.manager.PMMLManager;
import org.jpmml.model.ImportFilter;
import org.jpmml.model.JAXBUtil;
import org.jpmml.model.SourceLocationTransformer;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

/**
 *
 * @author yanni
 */
public class PmmlModel {

    private ModelEvaluator<?> modelEvaluator;
    private String modelFile;

    public ModelEvaluator<?> getModelEvaluator() {
        return modelEvaluator;
    }

    public String getModelFile() {
        return modelFile;
    }

    public PmmlModel(String modelFile) throws Exception {
        super();
        loadModel(modelFile);
    }

    private void loadModel(String modelFile) throws IOException, SAXException, JAXBException {
        PMML pmml;
        InputStream is = new FileInputStream(modelFile);
        try {
            Source source = ImportFilter.apply(new InputSource(is));
            pmml = JAXBUtil.unmarshalPMML(source);
        } finally {
            try {
                is.close();
            } catch (Exception e) {
            }
        }
        pmml.accept(new SourceLocationTransformer());
        PMMLManager pmmlManager = new PMMLManager(pmml);
        modelEvaluator = (ModelEvaluator<?>) pmmlManager.getModelManager(ModelEvaluatorFactory.getInstance());
    }

}
