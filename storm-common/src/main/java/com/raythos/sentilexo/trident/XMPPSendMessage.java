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

import java.util.Map;
import org.jivesoftware.smack.ConnectionConfiguration;
import org.jivesoftware.smack.XMPPConnection;
import org.jivesoftware.smack.XMPPException;
import org.jivesoftware.smack.packet.Message;
import org.jivesoftware.smack.packet.Message.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.operation.Filter;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

/**
 *
 * @author yanni
 */
public class XMPPSendMessage implements Filter{
       protected  static Logger log = LoggerFactory.getLogger(XMPPSendMessage.class);
       public static final String XMPP_TO = "storm.xmpp.to";
       public static final String XMPP_USER = "storm.xmpp.user";
       public static final String XMPP_PASSWORD = "storm.xmpp.password";
       public static final String XMPP_SERVER = "storm.xmpp.server";
       private XMPPConnection xmppConnection;
       private String to;
    //   private MessageMapper mapper;

    
    @Override
    public boolean isKeep(TridentTuple tuple) {
        Message msg = new Message(this.to, Type.normal);
        msg.setBody(tuple.toString());
        this.xmppConnection.sendPacket(msg);
        return true;
    }

    @Override
    public void prepare(Map conf, TridentOperationContext toc) {
        this.to = (String) conf.get(XMPP_TO);
        ConnectionConfiguration config = new ConnectionConfiguration((String) conf.get(XMPP_SERVER));
        this.xmppConnection = new XMPPConnection(config);
           try {
               this.xmppConnection.connect();
               this.xmppConnection.login((String) conf.get(XMPP_USER), (String) conf.get(XMPP_PASSWORD));
           } catch (XMPPException e) {
               log.warn("Error initializing XMPP Channel", e);
    }
    
    }

    @Override
    public void cleanup() {
     
    }
    
}
