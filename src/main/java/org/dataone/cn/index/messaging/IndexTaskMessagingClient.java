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

import org.dataone.cn.index.task.IndexTask;
import org.dataone.service.exceptions.InvalidSystemMetadata;
import org.dataone.service.exceptions.ServiceFailure;


/**
 * An interface of a client to submit an index task message to the message server
 * @author tao
 *
 */
public interface IndexTaskMessagingClient {
    
    //the key value in the header of messages.
    public static final String NODEID = "nodeId";
    public static final String FORMATTYPE = "formatType";
    public static final String PID = "pid";
    
    /**
     * Submit a index task to the message server
     * @param indexTask
     * @return
     * @throws ServiceFailure
     */
    public boolean submit(IndexTask indexTask) throws ServiceFailure, InvalidSystemMetadata;

}
