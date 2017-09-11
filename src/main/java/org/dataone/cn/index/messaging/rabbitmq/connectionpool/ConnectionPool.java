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
package org.dataone.cn.index.messaging.rabbitmq.connectionpool;

import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import com.rabbitmq.client.Connection;


/**
 * An object represents a pool of connections which can be reused. 
 * We are using apache common pool interface to manage the pool
 * @author tao
 *
 */
public class ConnectionPool extends GenericObjectPool<Connection>{

    /**
     * Constructor.
     * 
     * It uses the default configuration for pool provided by
     * apache-commons-pool2.
     * 
     * @param factory
     */
    public ConnectionPool(PooledObjectFactory<Connection> factory) {
        super(factory);
    }
    
    /**
     * Constructor.
     * 
     * This can be used to have full control over the pool using configuration
     * object.
     * 
     * @param factory
     * @param config
     */
    public ConnectionPool(PooledObjectFactory<Connection> factory,
            GenericObjectPoolConfig config) {
        super(factory, config);
    }

}
