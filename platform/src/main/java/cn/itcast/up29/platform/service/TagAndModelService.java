package cn.itcast.up29.platform.service;


import cn.itcast.up29.platform.entity.dto.ModelDto;
import cn.itcast.up29.platform.entity.dto.TagDto;
import cn.itcast.up29.platform.entity.dto.TagModelDto;

import java.util.List;

public interface TagAndModelService {
    /**
     * 根据123级添加标签
     * @param tags
     */
    void addTagsByRelation(List<TagDto> tags);

    /**
     * 根据等级查找
     * @return
     * @param level
     */
    List<TagDto> findByLevel(Integer level);


    /**
     * 根据ID查找
     * @return
     * @param pid
     */
    List<TagDto> findByPid(Long pid);


    /**
     * 新增标签模型
     */
    void addTagModel(TagDto tagDto, ModelDto modelDto);

    /**
     * 根据父ID查询模型
     * @param pid
     * @return
     */
    List<TagModelDto> findModelByPid(Long pid);

    /**
     * 添加5级标签数据
     * @param tagDto
     */
    void addDataTag(TagDto tagDto);

    void updateModelState(Long id, Integer state);
}
