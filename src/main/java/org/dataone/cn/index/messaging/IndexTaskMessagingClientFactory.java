/**
 * This work was created by participants in the DataONE project, and is
 * jointly copyrighted by participating institutions in DataONE. For 
 * more information on DataONE, see our web site at http://dataone.org.
 *
 *   Copyright ${year}
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and 
 * limitations under the License.
 * 
 * $Id$
 */
package org.dataone.cn.index.messaging;

import org.apache.log4j.Logger;
import org.dataone.configuration.Settings;


/**
 * The factory class to get the default IndexTaskMessagingClient object
 * @author tao
 *
 */
public class IndexTaskMessagingClientFactory {
    private static Logger logger = Logger.getLogger(IndexTaskMessagingClientFactory.class);
    
    /**
     * Get the default client (reading from the configuration file - messaging.peroperties)
     * @return an instance of the IndexTaskmessagingClient class.
     * @throws IllegalAccessException 
     * @throws InstantiationException 
     * @throws ClassNotFoundException 
     */
    public static IndexTaskMessagingClient getClient() throws InstantiationException, IllegalAccessException, ClassNotFoundException {
        String className = Settings.getConfiguration().getString("messaging.client.classname");
        logger.info("IndexTaskMessagingClientFactory.getClient - the default client class name (reading from the messaging.properties file is "+className);
        Class classDefinition = Class.forName(className);
        IndexTaskMessagingClient client = (IndexTaskMessagingClient) classDefinition.newInstance();
        return client;
    }

}
