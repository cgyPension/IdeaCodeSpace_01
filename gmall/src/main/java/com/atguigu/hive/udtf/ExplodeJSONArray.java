package com.atguigu.hive.udtf;


import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.json.JSONArray;

import java.util.ArrayList;
import java.util.List;

/**
 * @author GyuanYuan Cai
 * 2020/9/21
 * Description:
 */

@Description(name = "explode_json_array", value = " - explode json array .... edit by caiguangyaun")
public class ExplodeJSONArray extends GenericUDTF {

    /**
     * 作用:
     * 1.检测输入
     * a: 参数个数满足
     * b: 参数的类型
     * 2. 返回期望的数据类型的检测器
     */
    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
        //  TODO 对输入的数据做检测
        // 1.1 获取到传入的参数
        // explode_json_array(get_json_object(line, '$.actions'))
        List<? extends StructField> inputFields = argOIs.getAllStructFieldRefs();
        if (inputFields.size()!=1){
            throw new UDFArgumentException("explode_json_array 函数的参数个数必须是 1, 你现在传递的个是: "+inputFields.size());
        }

        ObjectInspector oi = inputFields.get(0).getFieldObjectInspector();
        if (oi.getCategory()!=ObjectInspector.Category.PRIMITIVE||!"string".equals(oi.getTypeName())){
            throw new UDFArgumentException("explode_json_array 函数的参数类型必须是string, 你现在传递的是: "+oi.getTypeName());
        }

        // TODO 2. 返回期望数据类型的检测器
        ArrayList<String> names = new ArrayList<>();
        names.add("item");
        ArrayList<ObjectInspector> ois = new ArrayList<>();
        ois.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(names, ois);
    }

    /**
     * 处理数据
     * [{}, {} ]  => {}, {}
     */
    @Override
    public void process(Object[] args) throws HiveException {
        // explode_json_array(get_json_object(line, '$.actions'))
        String jsonArrayString = args[0].toString();
        JSONArray jsonArray = new JSONArray(jsonArrayString);
        // 3 循环一次，取出数组中的一个json，并写出
        for (int i = 0; i < jsonArray.length(); i++) {
            String obj = jsonArray.getString(i);
            String[] cols = new String[1];
            cols[0]=obj;
            // 为什么要是数组?  主要是考虑, 炸裂之后, 每行会有可能是多列
            forward(cols);  // forward一次, 炸裂得到一行新数据
        }
    }

    // 关闭资源 不用实现
    @Override
    public void close() throws HiveException {

    }
}


/**
 * Description:
 */

