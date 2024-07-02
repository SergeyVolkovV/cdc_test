package org.example;


import java.io.Serializable;
import javax.json.*;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.*;

public class Tables implements Serializable{

    JsonObject jsonConf;
    List<String>  tableList;

    public static class Field implements Serializable{
        Integer ord;
        String name;
        String type;
        String system_field;
        String stateField;

        public Field(Integer ord, String name, String type, String system_field)
        {
            this.ord=ord;
            this.name = name;
            this.type = type;
            this.system_field=system_field;
            this.stateField="none";
        }

        public Field(Integer ord, String name, String type, String system_field, String stateField)
        {
            this.ord=ord;
            this.name = name;
            this.type = type;
            this.system_field=system_field;
            this.stateField=stateField;
        }

        @Override
        public String toString() {
            return "Field{" +
                    "ord=" + ord +
                    ", name='" + name + '\'' +
                    ", type='" + type + '\'' +
                    ", system_field='" + system_field + '\'' +
                    ", stateField='" + stateField + '\'' +
                    '}';
        }
    }

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
            setSqlMirror(table.asJsonObject());
            setSqlLog(table.asJsonObject());

        }

    }

    private void setSqlMirror(JsonObject table){

        String tableName = table.asJsonObject().getString("schema")+"."+table.asJsonObject().getString("name");
        String pkName = table.getString("pk");
        List<String> listUsingPart = new ArrayList<String>();
        List<String> listUpdatePart = new ArrayList<String>();
        List<String> listInsertPart = new ArrayList<String>();
        List<String> listInsertValuesPart = new ArrayList<String>();
        Integer ind = 0;
        List<Field> fields = new ArrayList<>();
        for (JsonValue field : table.getJsonArray("fields")) {

            String fieldName = field.asJsonObject().getString("field");
            listUsingPart.add("? as "+fieldName);
            if (!Objects.equals(fieldName, pkName))
                listUpdatePart.add("\n  "+fieldName+" = s."+fieldName);
            listInsertPart.add(fieldName);
            listInsertValuesPart.add("s."+fieldName);
            ind++;
            fields.add(new Field(ind,field.asJsonObject().getString("field"),getParamsType(field.asJsonObject().getString("type")) ,"false"));
        }
        fields.add(new Field(++ind,"dbz_op_type","string", "true"));



        StringBuilder sqlMirror =new StringBuilder();
        sqlMirror.append(String.format("MERGE INTO %s as t\n",tableName ));
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

    private void setSqlLog(JsonObject table){

        String tableName = table.asJsonObject().getString("schema")+"."+table.asJsonObject().getString("name");
        List<String> listInsertPart = new ArrayList<String>();
        List<String> listValuesPart = new ArrayList<String>();

        Integer ind = 0;
        List<Field> fields = new ArrayList<>();
        String temp = "INSERT INTO public.shipments_log\n" +
                                        "(before_shipment_id, before_order_id, before_origin, before_destination, before_is_arrived, after_shipment_id, after_order_id, after_origin, after_destination, after_is_arrived, op, dbz_ts_ms, src_ts_ms)\n" +
                                        "VALUES(?, 0, '', '', false, 0, 0, '', '', false, ?, ?, 0); ";
        for (JsonValue field : table.getJsonArray("fields")) {

            String fieldName = field.asJsonObject().getString("field");
            listInsertPart.add("before_"+fieldName);

            listValuesPart.add("? ");
            ind++;
            fields.add(new Field(ind,fieldName,getParamsType(field.asJsonObject().getString("type")) ,"false", "before"));
            listInsertPart.add("after_"+fieldName);

            listValuesPart.add("? ");
            ind++;
            fields.add(new Field(ind,fieldName,getParamsType(field.asJsonObject().getString("type")) ,"false","after"));

        }
        listInsertPart.add("dbz_op_type");
        listInsertPart.add("dbz_ts_ms");
        listInsertPart.add("src_ts_ms");

        fields.add(new Field(++ind,"dbz_op_type","string", "true","common"));
        fields.add(new Field(++ind,"dbz_ts_ms","long", "true","common"));
        fields.add(new Field(++ind,"src_ts_ms","long", "true","common"));

        listValuesPart.add("? ");
        listValuesPart.add("? ");
        listValuesPart.add("? ");


        StringBuilder sqlLog =new StringBuilder();
        sqlLog.append(String.format("INSERT INTO %s_log \n",tableName ));
        sqlLog.append(String.format("(%s)\n",String.join(",", listInsertPart) ));
        sqlLog.append("VALUES \n");
        sqlLog.append(String.format("(%s)\n",String.join(",", listValuesPart)));
        this.sqlLogInsertStatement.put(tableName,sqlLog.toString());
        this.sqlLogInsertParameters.put(tableName,fields);

    }

    public String getStringTableList(){
        return String.join(",", this.tableList);
    }



    private String getParamsType(String type){
        return type;
    }


}
