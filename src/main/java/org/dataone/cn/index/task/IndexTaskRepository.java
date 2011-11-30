package org.dataone.cn.index.task;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.transaction.annotation.Transactional;

@Transactional(readOnly = true)
public interface IndexTaskRepository extends JpaRepository<IndexTask, Long> {

}
