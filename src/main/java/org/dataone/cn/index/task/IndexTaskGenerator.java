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

package org.dataone.cn.index.task;

import java.net.URI;

import org.apache.log4j.Logger;

import org.dataone.service.types.v2.SystemMetadata;


/**
 * The generator will create the IndexTask objects from the given information
 * @author tao
 *
 */
public class IndexTaskGenerator {

    private static Logger logger = Logger.getLogger(IndexTaskGenerator.class.getName());
    

   /**
    * Generate an index task with the add priority
    * @param smd
    * @param objectURI
    * @return IndexTask
    */
    public IndexTask generateAddTask(SystemMetadata smd, String objectURI) {
        if (IgnoringIndexIdPool.isNotIgnorePid(smd)) {
            IndexTask task = new IndexTask(smd, objectURI);
            task.setAddPriority();
            String id = "Unknow";
            if(smd != null && smd.getIdentifier() != null) {
                id = smd.getIdentifier().getValue();
            }
            return task;
        }
        return null;
    }

    /**
     * Generate an index task with the update priority
     * @param smd
     * @param objectURI
     * @return indexTask
     */
    public IndexTask generateUpdateTask(SystemMetadata smd, String objectURI) {
        if (IgnoringIndexIdPool.isNotIgnorePid(smd)) {
            IndexTask task = new IndexTask(smd, objectURI);
            task.setUpdatePriority();
            String id = "Unknow";
            if(smd != null && smd.getIdentifier() != null) {
                id = smd.getIdentifier().getValue();
            }
            return task;
        }
        return null;
    }

   
    /**
     * Generate a deleting index task
     * @param smd
     * @return
     */
    public IndexTask generateDeleteTask(SystemMetadata smd) {
        if (IgnoringIndexIdPool.isNotIgnorePid(smd)) {
            IndexTask task = new IndexTask(smd, null);
            task.setDeleted(true);
//            String id = "Unknow";
//            if(smd != null && smd.getIdentifier() != null) {
//                id = smd.getIdentifier().getValue();
//            }            
            return task;
        }
        return null;
    }


    /**
     * Generate an index task with the given priority
     * @param smd
     * @param objectURI
     * @return indexTask
     */
    public IndexTask generate(SystemMetadata smd, String objectURI, int priority) {
        if (IgnoringIndexIdPool.isNotIgnorePid(smd)) {
            IndexTask task = new IndexTask(smd, objectURI);
            task.setPriority(priority);
//            String id = "Unknow";
//            if(smd != null && smd.getIdentifier() != null) {
//                id = smd.getIdentifier().getValue();
//            }
            return task;
        }
        return null;
    }
 
}
