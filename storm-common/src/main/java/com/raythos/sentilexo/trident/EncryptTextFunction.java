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
import java.io.UnsupportedEncodingException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.logging.Level;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.KeyGenerator;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

/**
 *
 * @author yanni
 */
@SuppressWarnings({"serial", "rawtypes"})
public class EncryptTextFunction extends BaseFunction {

    protected static Logger log = LoggerFactory.getLogger(EncryptTextFunction.class);
        private static SecretKey secretKey = null;
        private static Cipher cipher = null;
 
    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        KeyGenerator keyGenerator = null;
        try {
            keyGenerator = KeyGenerator.getInstance("DESede");
            keyGenerator.init(168);
            secretKey = keyGenerator.generateKey();
            cipher = Cipher.getInstance("DESede");
  
        } catch (NoSuchAlgorithmException ex) {
            log.error("Error whilst setting up encryption algorith. The exception was: ", ex);
        }
        catch (NoSuchPaddingException ex) {
            log.error("Error whilst setting up cipher. The exception was: ", ex);

        }

    }

    @Override
    public void cleanup() {
    }

    @Override
    public void execute(TridentTuple tuple, TridentCollector tc) {
        try {
            String text = tuple.getString(0);
            byte[] textBytes = text.getBytes("UTF8");
            cipher.init(Cipher.ENCRYPT_MODE, secretKey);
            byte[] cipherBytes = cipher.doFinal(textBytes);
            String cipherText = new String(cipherBytes, "UTF8");
            tc.emit(new Values(cipherText));
         } catch (UnsupportedEncodingException | InvalidKeyException | IllegalBlockSizeException ex) {
             log.error("Error whilst ecncrypting. The exception was: ", ex);        
         } catch (BadPaddingException ex) {
          log.error("Error whilst ecncrypting and padding. The exception was: ", ex);
         }
        
    }

}
