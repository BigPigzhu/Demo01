package cn.itcast.up29.platform.service.impl;

import cn.itcast.up29.platform.entity.dto.ModelDto;
import cn.itcast.up29.platform.entity.dto.TagDto;
import cn.itcast.up29.platform.entity.dto.TagModelDto;
import cn.itcast.up29.platform.entity.po.ModelPo;
import cn.itcast.up29.platform.entity.po.TagPo;
import cn.itcast.up29.platform.repo.ModelRepo;
import cn.itcast.up29.platform.repo.TagRepo;
import cn.itcast.up29.platform.service.Engine;
import cn.itcast.up29.platform.service.TagAndModelService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

@Service
public class TagAndModelServiceImpl implements TagAndModelService {

    @Autowired
    private TagRepo tagRepo;

    @Autowired
    private ModelRepo modelRepo;

    @Autowired
    private Engine engine;


    @Override
    public void addTagsByRelation(List<TagDto> tags) {
        //先排序
        tags.sort((o1, o2) -> {
            if (o1.getLevel() > o2.getLevel()) {
                return 1;
            }
            if (o1.getLevel() < o2.getLevel()) {
                return -1;
            }
            return 0;
        });
        //长度等于3才符合录入条件
        if (tags.size() == 3) {
            TagDto tagDto1 = tags.get(0);
            String level1Name = tagDto1.getName();

        }

        TagPo tmp = null;
        for (TagDto tagDto : tags) {
            //将dto转换为po
            TagPo tagPo = convert(tagDto);
            String name = tagPo.getName();
            Integer level = tagPo.getLevel();
            //如果当前对象没有父ID,那么就将上次保存的ID赋值过去.
            if (tagPo.getPid() == null && tmp != null) {
                tagPo.setPid(tmp.getId());
            }
            Long pid = tagPo.getPid();
            List<TagPo> tagList = tagRepo.findByNameAndLevelAndPid(name, level, pid);
            if (tagList == null || tagList.size() == 0) {
                //没找到,那就添加一级,
                tmp = tagRepo.save(tagPo);
            } else {
                //如果找到了,就直接获取第一个(也只能有1个.)
                tmp = tagList.get(0);
            }
        }

    }

    @Override
    public List<TagDto> findByLevel(Integer level) {
        List<TagPo> levelList = tagRepo.findByLevel(level);
        //将tagpo集合转换为tagDto集合
        return levelList.stream().map(this::convert).collect(Collectors.toList());
    }

    @Override
    public List<TagDto> findByPid(Long pid) {
        List<TagPo> list = tagRepo.findByPid(pid);
        //将tagpo集合转换为tagDto集合
        List<TagDto> collect = list.stream().map(this::convert).collect(Collectors.toList());
        return collect;
    }

    @Override
    public void addTagModel(TagDto tagDto, ModelDto modelDto) {
        TagPo tagPo = tagRepo.save(convert(tagDto));
        modelRepo.save(convert(modelDto, tagPo.getId()));
    }

    @Override
    public List<TagModelDto> findModelByPid(Long pid) {
        List<TagPo> tagPos = tagRepo.findByPid(pid);
        return tagPos.stream().map((tagPo) -> {
            Long id = tagPo.getId();
            ModelPo modelPo = modelRepo.findByTagId(id);
            if (modelPo == null) {
                //找不到model,就只返回tag
                return new TagModelDto(convert(tagPo),null);
            }
            return new TagModelDto(convert(tagPo), convert(modelPo));
        }).collect(Collectors.toList());
    }

    @Override
    public void addDataTag(TagDto tagDto) {
        tagRepo.save(convert(tagDto));
    }

    @Override
    public void updateModelState(Long id, Integer state) {
        ModelPo modelPo = modelRepo.findByTagId(id);

        if (state == ModelPo.STATE_ENABLE) {
            //启动流程
            engine.startModel(convert(modelPo));
        }
        if (state == ModelPo.STATE_DISABLE) {
            //关闭流程
            engine.stopModel(convert(modelPo));
        }
        //更新状态信息
        modelPo.setState(state);
        modelRepo.save(modelPo);
    }

    private ModelDto convert(ModelPo modelPo) {
        ModelDto modelDto = new ModelDto();
        modelDto.setId(modelPo.getId());
        modelDto.setName(modelPo.getName());
        modelDto.setMainClass(modelPo.getMainClass());
        modelDto.setPath(modelPo.getPath());
        modelDto.setArgs(modelPo.getArgs());
        modelDto.setState(modelPo.getState());
        modelDto.setSchedule(modelDto.parseDate(modelPo.getSchedule()));

        return modelDto;
    }

    private ModelPo convert(ModelDto modelDto, Long id) {
        ModelPo modelPo = new ModelPo();
        modelPo.setId(modelDto.getId());
        modelPo.setTagId(id);
        modelPo.setName(modelDto.getName());
        modelPo.setMainClass(modelDto.getMainClass());
        modelPo.setPath(modelDto.getPath());
        modelPo.setSchedule(modelDto.getSchedule().toPattern());
        modelPo.setCtime(new Date());
        modelPo.setUtime(new Date());
        modelPo.setState(modelDto.getState());
        modelPo.setArgs(modelDto.getArgs());

        return modelPo;
    }


    private TagDto convert(TagPo tagPo) {
        TagDto tagDto = new TagDto();
        tagDto.setId(tagPo.getId());
        tagDto.setPid(tagPo.getPid());
        tagDto.setLevel(tagPo.getLevel());
        tagDto.setName(tagPo.getName());
        tagDto.setRule(tagPo.getRule());

        return tagDto;
    }

    private TagPo convert(TagDto tagDto) {
        TagPo tagPo = new TagPo();
        tagPo.setId(tagDto.getId());
        tagPo.setPid(tagDto.getPid());
        tagPo.setLevel(tagDto.getLevel());
        //如果当前等级为1级,设置父ID为-1
        if (tagDto.getLevel() == 1) {
            tagPo.setPid(-1L);
        }
        tagPo.setName(tagDto.getName());
        tagPo.setRule(tagDto.getRule());
        tagPo.setCtime(new Date());
        tagPo.setUtime(new Date());
        return tagPo;
    }
}
