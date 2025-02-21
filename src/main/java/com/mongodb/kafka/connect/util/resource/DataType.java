package com.mongodb.kafka.connect.util.resource;

import java.util.Arrays;
import java.util.List;

public enum DataType {
  STRING("String", "字符串"),
  INTEGER("Integer", "整数"),
  BOOLEAN("Boolean", "布尔值"),
  DOUBLE("Double", "浮点数"),
  MIN_MAX_KEYS("Min/Max Keys", "最小/最大值"),
  LIST("List", "对象列表"),
  ARRAY("Array", "数组"),
  TIMESTAMP("Timestamp", "时间戳"),
  OBJECT("Object", "内嵌对象"),
  NULL("Null", "Null值"),
  SYMBOL("Symbol", "符号"),
  DATE("Date", "日期时间"),
  OBJECT_ID("Object Id", "对象编号"),
  BINARY_DATA("Binary Data", "二进制数据"),
  CODE("Code", "代码"),
  REGULAR_EXPRESSION("Regular Express", "正则表达式"),
  GEOSPATIAL("Geo Spatial", "地理位置信息");

  private String englishName;

  private String chineseName;

  private static List<DataType> arrayNestedDataTypes =
      Arrays.asList(STRING, INTEGER, BOOLEAN, DOUBLE, DATE, OBJECT_ID);

  DataType(String en_Name, String cn_Name) {
    this.englishName = en_Name;
    this.chineseName = cn_Name;
  }

  public String getEnglishName() {
    return englishName;
  }

  public void setEnglishName(String englishName) {
    this.englishName = englishName;
  }

  public String getChineseName() {
    return chineseName;
  }

  public void setChineseName(String chineseName) {
    this.chineseName = chineseName;
  }

  public String getDisplayName(Language language) {
    if (language == Language.en) {
      return this.englishName;
    } else if (language == Language.cn) {
      return this.chineseName;
    }
    return this.englishName;
  }

  public static List<DataType> getArrayNestedDataTypes() {
    return DataType.arrayNestedDataTypes;
  }
}
