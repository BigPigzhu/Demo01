package cn.itcast.up29.platform.repo;

import cn.itcast.up29.platform.entity.po.ModelPo;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ModelRepo extends JpaRepository<ModelPo, Long> {

    ModelPo findByTagId(Long tagId);


}
