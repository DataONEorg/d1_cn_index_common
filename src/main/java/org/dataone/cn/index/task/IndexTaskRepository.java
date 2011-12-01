package org.dataone.cn.index.task;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.transaction.annotation.Transactional;

@Transactional(readOnly = true)
public interface IndexTaskRepository extends JpaRepository<IndexTask, Long> {

    List<IndexTask> findByPid(String pid);

    List<IndexTask> findByPidAndStatus(String pid, String status);
}
