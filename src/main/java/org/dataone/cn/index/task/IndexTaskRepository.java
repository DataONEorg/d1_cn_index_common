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

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.transaction.annotation.Transactional;

/**
 * IndexTaskRepository is an extension of spring-data JpaRepository and
 * represents the DAO layer for IndexTask object.
 * 
 * @author sroseboo
 * 
 */
@Transactional(readOnly = true)
public interface IndexTaskRepository extends JpaRepository<IndexTask, Long> {

    /**
     * Return a List of IndexTask objects whose pid value matches the pid
     * parameter value.
     * 
     * @param pid
     * @return
     */
    List<IndexTask> findByPid(String pid);

    /**
     * Return a List of IndexTask objects whose pid and status values match the
     * corresponding parameter values.
     * 
     * @param pid
     * @param status
     *            - a constant defined by the IndexTask class.
     * @return
     */
    List<IndexTask> findByPidAndStatus(String pid, String status);

    /**
     * Return a List of IndexTasks that have a matching status and order the
     * results by priority and modified date
     * 
     * @param status
     * @return
     */
    List<IndexTask> findByStatusOrderByPriorityAscTaskModifiedDateAsc(
            String status);

}
