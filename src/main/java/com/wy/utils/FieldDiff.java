package com.wy.utils;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.AlterTableEvent;
import org.apache.hadoop.hive.metastore.events.PreAlterTableEvent;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * 比较表字段变化的算法类
 */
public class FieldDiff {
    //新增的字段
    public List<FieldSchema> addedFields = new ArrayList<>();
    //删除的字段
    public List<FieldSchema> deletedFields = new ArrayList<>();
    //发生了改变的字段
    public List<FieldSchema> modifiedFields = new ArrayList<>();
    //没有发生改变的字段
    public List<FieldSchema> unchangedFields = new ArrayList<>();
    //新表字段列表
    public String fieldNames;

    private List<FieldSchema> oldtable_fields = null;
    private List<FieldSchema> newtable_fields = null;

    public FieldDiff(AlterTableEvent tableEvent) {
        this.oldtable_fields = getAllFields(tableEvent.getOldTable());
        this.newtable_fields = getAllFields(tableEvent.getNewTable());
        this.compareFieldLists(oldtable_fields, newtable_fields);
        //处理出新表字段的列表
        StringBuilder tmp = new StringBuilder();
        newtable_fields.stream().map( f -> f.getName() ).forEach(name -> tmp.append(name).append(","));
        tmp.deleteCharAt(tmp.length()-1);
        this.fieldNames = tmp.toString();
    }

    public FieldDiff(PreAlterTableEvent tableEvent) {
        this.oldtable_fields = getAllFields(tableEvent.getOldTable());
        this.newtable_fields = getAllFields(tableEvent.getNewTable());
        this.compareFieldLists(oldtable_fields, newtable_fields);
        //处理出新表字段的列表
        StringBuilder tmp = new StringBuilder();
        newtable_fields.stream().map( f -> f.getName() ).forEach(name -> tmp.append(name).append(","));
        tmp.deleteCharAt(tmp.length()-1);
        this.fieldNames = tmp.toString();
    }

    /**
     * 解析出表的所有字段，普通字段 + 分区字段
     * @param table
     * @return
     */
    private List<FieldSchema> getAllFields(Table table) {
        List<FieldSchema> allFields = new ArrayList<>();

        // 添加普通字段
        if (table.getSd() != null && table.getSd().getCols() != null) {
            allFields.addAll(table.getSd().getCols());
        }

        // 添加分区字段
        if (table.getPartitionKeys() != null) {
            allFields.addAll(table.getPartitionKeys());
        }

        return allFields;
    }

    /**
     * 比较的核心方法
     * @param oldtable_fields
     * @param newtable_fields
     */
    private void compareFieldLists(List<FieldSchema> oldtable_fields, List<FieldSchema> newtable_fields){
        // 创建字段名到字段对象的映射
        Map<String, FieldSchema> oldFieldMap = createFieldMap(oldtable_fields);
        Map<String, FieldSchema> newFieldMap = createFieldMap(newtable_fields);

        // 1. 检测删除的字段（在旧表但不在新表）
        for (FieldSchema oldField : oldtable_fields) {
            if (!newFieldMap.containsKey(oldField.getName())) {
                this.deletedFields.add(oldField);
            }
        }

        // 2. 检测新增和修改的字段
        for (FieldSchema newField : newtable_fields) {
            FieldSchema oldField = oldFieldMap.get(newField.getName());

            if (oldField == null) {
                // 不存在于旧字段map容器中，则为新增字段
                this.addedFields.add(newField);
            } else {
                // 检查字段是否被修改，由于比较方法中只比较了字段名
                // 因此这里并不会有任何老字段被判定为修改
                if (isFieldModified(oldField, newField)) {
                    this.modifiedFields.add(newField);
                } else {
                    this.unchangedFields.add(newField);
                }
            }
        }

    }

    /**
     * 重构字段信息数据为Map集合的方法
     * @param fields
     * @return
     */
    private Map<String, FieldSchema> createFieldMap(List<FieldSchema> fields) {
        Map<String, FieldSchema> map = new HashMap<>();
        for (FieldSchema field : fields) {
            map.put(field.getName(), field);
        }
        return map;
    }

    /**
     * 比较两个字段是否因不一样，从而要做出相关操作
     * @param oldField
     * @param newField
     * @return 如果两个比较字段是一样的，就返回false，意味着不需要任何操作，反之返回true
     */
    private boolean isFieldModified(FieldSchema oldField, FieldSchema newField) {
        // 比较名字
        if (!equalsWithNullCheck(oldField.getName(), newField.getName())) {
            return true;
        }

        // 可以根据需要添加更多比较项

        return false;
    }

    /**
     * 最终的比较方法，考虑空值
     * @param o1
     * @param o2
     * @return
     */
    private boolean equalsWithNullCheck(Object o1, Object o2) {
        if (o1 == null && o2 == null) return true;
        if (o1 == null || o2 == null) return false;
        return o1.equals(o2);
    }

}
