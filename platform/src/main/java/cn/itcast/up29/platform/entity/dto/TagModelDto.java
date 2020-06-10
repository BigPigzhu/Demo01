package cn.itcast.up29.platform.entity.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class TagModelDto {
    private TagDto tag;
    private ModelDto model;
}
