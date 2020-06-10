package cn.itcast.up29.platform.entity.dto;

import lombok.Data;

@Data
public class TagDto {
    private Long id;
    private Long pid;
    private Integer level;
    private String name;
    //如果是四/五级标签,涉及到规则
    private String rule;
}
