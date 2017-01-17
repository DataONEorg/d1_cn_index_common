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

import org.dataone.service.types.v2.SystemMetadata;

/**
 * Represents a pool contains all identifiers which will be ignored in the index process.
 * @author tao
 *
 */
public class IgnoringIndexIdPool {
    /**
     * the pid of the ObjectFormatList is OBJECT_FORMAT_LIST.1.{version}, so
     * the .1 could be seen as part of the prefix.  For simplicity, and to guard 
     * against possible minor change in the future, we will just use a shorter prefix - 
     * without the '.1'
     * Note: the prefix is maintained in 
     * cn-buildout/dataone-cn-metacat/usr/share/metacata/debian/insertOrUpdateObjectFormatList.sh
     */
    private static final String IGNOREPIDPREFIX = "OBJECT_FORMAT_LIST.";
    
    /**
     * If the identifier on the given system metadata object should be not ignore
     * @param smd
     * @return true if it shouldn't be ignored; otherwise false
     */
    public static boolean isNotIgnorePid(SystemMetadata smd) {
        if(smd != null && smd.getIdentifier() != null && smd.getIdentifier().getValue() != null && 
                smd.getIdentifier().getValue().startsWith(IGNOREPIDPREFIX)) {
            return false;
        }
        return true;
    }
}
