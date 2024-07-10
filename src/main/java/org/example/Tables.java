package org.example;


import java.io.Serializable;
import javax.json.*;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.*;

/*TODO read conf from s3*/
public class Tables implements Serializable{

    JsonObject jsonConf;
    List<String>  tableList;
    String tablesSource;

    public static class Field implements Serializable{
        Integer ord;
        String name;
        String type;
        String stateField;

        public Field(Integer ord, String name, String type, String stateField)
        {
            this.ord=ord;
            this.name = name;
            this.type = type;
            this.stateField=stateField;
        }



        @Override
        public String toString() {
            return "Field{" +
                    "ord=" + ord +
                    ", name='" + name + '\'' +
                    ", type='" + type + '\'' +
                    ", stateField='" + stateField + '\'' +
                    '}';
        }
    }

    HashMap<String,Properties> tableProperties = new HashMap<>();
    HashMap<String,String> sqlMergeStatement =new HashMap<>();
    HashMap<String, List<Field>> sqlMergeParameters = new HashMap<>();
    HashMap<String,String> sqlLogInsertStatement =new HashMap<>();
    HashMap<String, List<Field>> sqlLogInsertParameters = new HashMap<>();


    public Tables() throws Exception {
        this.jsonConf=readJson();
        setTableList();
        setSinkDefinition();

    }

    public Tables(String path) throws Exception {
        this.jsonConf=readJson(path);
        setTableList();
        setSinkDefinition();

    }

    public static void main(String[] args) throws Exception {

        Tables t = new Tables();
        System.out.println(t.jsonConf.toString());
        System.out.println(t.getStringTableList());
        System.out.println(t.sqlLogInsertStatement);
        System.out.println(t.sqlLogInsertParameters);

    }

    private  JsonObject readJson() throws Exception
    {
        InputStream fis = new FileInputStream("./conf/conf.json");
        JsonReader reader = Json.createReader(fis);
        JsonObject conf = reader.readObject();
        reader.close();
        return conf;
    }

    private  JsonObject readJson(String path) throws Exception
    {
        InputStream fis = new FileInputStream(path);
        JsonReader reader = Json.createReader(fis);
        JsonObject conf = reader.readObject();
        reader.close();
        return conf;
    }

    private  void setTableList()
    {
        tablesSource = jsonConf.getString("source");
        JsonArray tables = jsonConf.getJsonArray("tables");
        List<String> list = new ArrayList<>();
        for (JsonValue table : tables) {
            list.add(table.asJsonObject().getString("schema")+"."+table.asJsonObject().getString("name"));

        }
        this.tableList = list;

    }

    private void setSinkDefinition()
    {
        JsonArray tables = jsonConf.getJsonArray("tables");
        for (JsonValue table : tables) {
            setTableProperties(table.asJsonObject());
            setSqlMirror(table.asJsonObject());
            setSqlLog(table.asJsonObject());
        }

    }

    private void setSqlMirror(JsonObject table){
        String tableName = table.asJsonObject().getString("schema")+"."+table.asJsonObject().getString("name");
        Properties tableProp = tableProperties.get(tableName);
        if (tableProp.get("target_table_mirror")!="")
        {
            String pkName = table.getString("pk");
            List<String> listUsingPart = new ArrayList<String>();
            List<String> listUpdatePart = new ArrayList<String>();
            List<String> listInsertPart = new ArrayList<String>();
            List<String> listInsertValuesPart = new ArrayList<String>();
            Integer ind = 0;
            List<Field> fields = new ArrayList<>();
            for (JsonValue field : table.getJsonArray("fields")) {

                String fieldName = field.asJsonObject().getString("field");
                String fieldType = field.asJsonObject().getString("type");
                switch (fieldType) {
                    case "timestamp":
                        listUsingPart.add("?::timestamp as " + fieldName);
                        break;
                    case "timestamptz":
                        listUsingPart.add("?::timestamptz as " + fieldName);
                        break;
                    case "date":
                        listUsingPart.add("?::date as " + fieldName);
                        break;
                    default:
                        listUsingPart.add("? as " + fieldName);
                        break;
                }
                if (!Objects.equals(fieldName, pkName))
                    listUpdatePart.add("\n  "+fieldName+" = s."+fieldName);
                listInsertPart.add(fieldName);
                listInsertValuesPart.add("s."+fieldName);
                ind++;
                fields.add(new Field(ind,field.asJsonObject().getString("field"),getParamsType(field.asJsonObject().getString("type")) ,"none"));
            }
            fields.add(new Field(++ind,"dbz_op_type","string", "common"));



            StringBuilder sqlMirror =new StringBuilder();
            sqlMirror.append(String.format("MERGE INTO %s as t\n",tableProp.get("target_table_mirror") ));
            sqlMirror.append(String.format("USING (select %s ,? as dbz_op_type ) as  s\n",String.join(",", listUsingPart) ));
            sqlMirror.append(String.format("ON t.%s = s.%s \n",pkName,pkName ));
            sqlMirror.append("WHEN MATCHED AND dbz_op_type='d' THEN \n");
            sqlMirror.append("  DELETE\n");
            sqlMirror.append("WHEN MATCHED THEN \n  UPDATE SET ");
            sqlMirror.append(String.join(",", listUpdatePart));
            sqlMirror.append("\nWHEN NOT MATCHED THEN \n");
            sqlMirror.append(String.format("INSERT (%s) \n",String.join(",", listInsertPart) ));
            sqlMirror.append(String.format("VALUES (%s) \n",String.join(",", listInsertValuesPart) ));
            this.sqlMergeStatement.put(tableName,sqlMirror.toString());
            this.sqlMergeParameters.put(tableName,fields);
        }
    }

    private void setSqlLog(JsonObject table){

        String tableName = table.asJsonObject().getString("schema")+"."+table.asJsonObject().getString("name");
        Properties tableProp = tableProperties.get(tableName);
        if (tableProp.get("target_table_log")!="") {

            List<String> listInsertPart = new ArrayList<String>();
            List<String> listValuesPart = new ArrayList<String>();

            Integer ind = 0;
            List<Field> fields = new ArrayList<>();
            for (JsonValue field : table.getJsonArray("fields")) {

                String fieldName = field.asJsonObject().getString("field");
                String fieldType = field.asJsonObject().getString("type");
                listInsertPart.add("before_" + fieldName);
                switch (fieldType) {
                    case "timestamp":
                        listValuesPart.add("?::timestamp " );
                        break;
                    case "timestamptz":
                        listValuesPart.add("?::timestamptz " );
                        break;
                    case "date":
                        listValuesPart.add("?::date " );
                        break;
                    default:
                        listValuesPart.add("? ");
                        break;
                }

                ind++;
                fields.add(new Field(ind, fieldName, getParamsType(field.asJsonObject().getString("type")), "before"));
                listInsertPart.add("after_" + fieldName);

                switch (fieldType) {
                    case "timestamp":
                        listValuesPart.add("?::timestamp " );
                        break;
                    case "timestamptz":
                        listValuesPart.add("?::timestamptz " );
                        break;
                    case "date":
                        listValuesPart.add("?::date " );
                        break;
                    default:
                        listValuesPart.add("? ");
                        break;
                }
                ind++;
                fields.add(new Field(ind, fieldName, getParamsType(field.asJsonObject().getString("type")), "after"));

            }
            listInsertPart.add("dbz_op_type");
            listInsertPart.add("dbz_ts_ms");
            listInsertPart.add("src_ts_ms");

            fields.add(new Field(++ind, "dbz_op_type", "string", "common"));
            fields.add(new Field(++ind, "dbz_ts_ms", "long", "common"));
            fields.add(new Field(++ind, "src_ts_ms", "long", "common"));

            listValuesPart.add("? ");
            listValuesPart.add("? ");
            listValuesPart.add("? ");


            StringBuilder sqlLog = new StringBuilder();
            sqlLog.append(String.format("INSERT INTO %s \n", tableProp.get("target_table_log")));
            sqlLog.append(String.format("(%s)\n", String.join(",", listInsertPart)));
            sqlLog.append("VALUES \n");
            sqlLog.append(String.format("(%s)\n", String.join(",", listValuesPart)));
            this.sqlLogInsertStatement.put(tableName, sqlLog.toString());
            this.sqlLogInsertParameters.put(tableName, fields);
        }

    }

    public String getStringTableList(){
        return String.join(",", this.tableList);
    }



    private String getParamsType(String type){
        return type;
    }

    private void setTableProperties(JsonObject table) {
        String tableName = table.asJsonObject().getString("schema")+"."+table.asJsonObject().getString("name");
        Properties prop = new Properties();
        System.out.println(table);
        if (table.asJsonObject().containsKey("target")) {
            JsonObject tableTarget = table.asJsonObject().get("target").asJsonObject();
            prop.put("target_name",getJsonValue(tableTarget,"target_name"));
            prop.put("target_table_mirror",getJsonValue(tableTarget,"target_table_mirror"));
            prop.put("target_table_log",getJsonValue(tableTarget,"target_table_log"));

        }
        tableProperties.put(tableName,prop);


    }

    private static String getJsonValue(JsonObject obj, String key) {
        String value="";
        if ( obj.containsKey(key)&&!obj.isNull(key))
            value= obj.getString(key);
        else
            value="" ;
        return value;
    }


}
