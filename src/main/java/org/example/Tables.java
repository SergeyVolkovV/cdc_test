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
        String source_name;
        String target_name;
        String type;
        String stateField;



        public Field(Integer ord, String source_name, String target_name, String type, String stateField) {
            this.ord = ord;
            this.source_name = source_name;
            this.target_name = target_name;
            this.type = type;
            this.stateField = stateField;
        }


        @Override
        public String toString() {
            return "Field{" +
                    "ord=" + ord +
                    ", source_name='" + source_name + '\'' +
                    ", target_name='" + target_name + '\'' +
                    ", type='" + type + '\'' +
                    ", stateField='" + stateField + '\'' +
                    '}';
        }
    }

    public static class Target implements Serializable {
        String targetName;
        String targetTable;
        String targetNotation;
        String targetType;
        List<String> targetFields;
        List<String> mapFields;
        List<Field> fields = new ArrayList<>();
        String sqlStatement;

        private final JsonArray sourceFields;
        private final String pkField;


        public Target(String targetName, String targetTable, String targetNotation, String targetType,
                      List<String> targetFields, List<String> mapFields, JsonArray sourceFields, String pkField) {
            this.targetName = targetName;
            this.targetTable = targetTable;
            if (Objects.equals(targetNotation, ""))
                this.targetNotation = "postgresql";
            else
                this.targetNotation = targetNotation;
            if (Objects.equals(targetType, ""))
                this.targetType = "mirror";
            else
                this.targetType = targetType;
            this.targetFields = targetFields;
            this.mapFields = mapFields;
            this.sourceFields=sourceFields;
            this.pkField=pkField;
            setColumnsMap();
            setSinkSql();

        }


        @Override
        public String toString() {
            return "Target{" +
                    "targetName='" + targetName + '\'' +
                    ", targetTable='" + targetTable + '\'' +
                    ", targetNotation='" + targetNotation + '\'' +
                    ", targetType='" + targetType + '\'' +
                    ", targetFields=" + targetFields +
                    ", mapFields=" + mapFields +
                    ", sqlStatement='" + sqlStatement + '\'' +
                    '}';
        }

        private void setSinkSql() {
            if (Objects.equals(targetNotation, "postgresql"))
            {

                switch (targetType){
                    case "mirror":
                        setSqlMirrorPostgres();
                        break;
                    case "log":
                        setSqlLogPostgres();
                        break;
                }
            }

            if (Objects.equals(targetNotation, "oracle"))
            {

                switch (targetType){
                    case "mirror":
                        setSqlMirrorOracle();
                        break;
                    case "log":
                        setSqlLogOracle();
                        break;
                }
            }
        }

        private void setColumnsMap(){
            String after_prefix="after_";
            String before_prefix="before_";
            boolean all_after = false;
            boolean all_before = false;
            int indField=1;
            if (Objects.equals(targetType, "mirror"))
            {

                 after_prefix="";
                 before_prefix="";

            }

            if (!mapFields.isEmpty()) {
                List<List<String>> listsFields = getListByIndMap(mapFields);
                /*all check */
                int ind=0;
                for (String s: listsFields.get(1)){
                    if (Objects.equals(s, "*")) {
                       if (Objects.equals(listsFields.get(0).get(ind), "before"))
                       {
                           all_before=true;
                           before_prefix = listsFields.get(2).get(ind).replace("*","");
                       }
                        if (Objects.equals(listsFields.get(0).get(ind), "after"))
                        {
                            all_after=true;
                            after_prefix = listsFields.get(2).get(ind).replace("*","");
                        }
                    }
                    ind++;

                }


                /*get defined before fields*/
                if (!all_before){
                    ind=0;
                    for (String stateType: listsFields.get(0)) {

                        if (Objects.equals(stateType, "before")&&(targetFields.isEmpty()||targetFields.contains(listsFields.get(2).get(ind)))) {
                            fields.add(new Field(indField++,
                                    listsFields.get(1).get(ind),
                                    listsFields.get(2).get(ind),
                                    getTypeSourceColumn(listsFields.get(1).get(ind)),
                                    "before"
                                    ));
                        }
                        ind++;
                    }
                }
                /*get defined before fields*/
                if (!all_after){
                    ind=0;
                    for (String stateType: listsFields.get(0)) {

                        if (Objects.equals(stateType, "after")&&(targetFields.isEmpty()||targetFields.contains(listsFields.get(2).get(ind)))) {
                            fields.add(new Field(indField++,
                                    listsFields.get(1).get(ind),
                                    listsFields.get(2).get(ind),
                                    getTypeSourceColumn(listsFields.get(1).get(ind)),
                                    "after"
                            ));
                        }
                        ind++;
                    }
                }
            }
            else
            {
                all_before = !Objects.equals(targetType, "mirror");
                all_after = true;
            }
            /*get all before fields*/
            if (all_before) {
                for (JsonValue field : sourceFields)
                {
                    String fieldName = field.asJsonObject().getString("field");
                    String fieldType = field.asJsonObject().getString("type");
                    if (targetFields.isEmpty()||targetFields.contains(before_prefix+fieldName))
                        fields.add(new Field(indField++,
                                fieldName,
                                before_prefix+fieldName,
                                fieldType,
                                "before"
                        ));
                }
            }
            /*get all after fields*/
            if (all_after) {
                for (JsonValue field : sourceFields)
                {
                    String fieldName = field.asJsonObject().getString("field");
                    String fieldType = field.asJsonObject().getString("type");
                    if (targetFields.isEmpty()||targetFields.contains(after_prefix+fieldName))
                        fields.add(new Field(indField++,
                                fieldName,
                                after_prefix+fieldName,
                                fieldType,
                                "after"
                        ));
                }
            }

            /*system fields*/
            fields.add(new Field(indField++,"dbz_op_type","dbz_op_type","string", "common"));

            if (Objects.equals(targetType, "log"))
            {
                fields.add(new Field(indField++,"dbz_ts_ms","dbz_ts_ms","long", "common"));
                fields.add(new Field(indField,"src_ts_ms","src_ts_ms","long", "common"));

            }


        }

        private String getTypeSourceColumn(String sourceName){
            for (JsonValue field : sourceFields) {
                String fieldName = field.asJsonObject().getString("field");
                String fieldType = field.asJsonObject().getString("type");
                if (Objects.equals(fieldName, sourceName))
                    return fieldType;
            }
            return "unknown";
        }

        private List<List<String>> getListByIndMap(List<String> list){
            List<String> indList1 = new ArrayList<>();
            List<String> indList2 = new ArrayList<>();
            List<String> indList3 = new ArrayList<>();
            for (String l: list)
            {
                indList1.add(l.split(":")[0].split("\\.")[0]);
                indList2.add(l.split(":")[0].split("\\.")[1]);
                indList3.add(l.split(":")[1]);
            }
            return  Arrays.asList(indList1, indList2, indList3);
        }

        private void setSqlMirrorPostgres(){

            List<String> listUsingPart = new ArrayList<>();
            List<String> listUpdatePart = new ArrayList<>();
            List<String> listInsertPart = new ArrayList<>();
            List<String> listInsertValuesPart = new ArrayList<>();
            String pkTargetName=this.pkField;
            for (Field field : fields) {
                    String castVar = castPostgres(field.type);
                    listUsingPart.add(castVar + " as " + field.source_name);
                if (!Objects.equals(field.stateField, "common")) {

                        if (!Objects.equals(field.source_name, this.pkField)) {
                            listUpdatePart.add("\n  " + field.target_name + " = s." + field.source_name);
                        }
                        else
                        {
                            pkTargetName = field.target_name;
                        }


                        listInsertPart.add(field.target_name);
                        listInsertValuesPart.add("s." + field.source_name);
                    }
            }




            this.sqlStatement= String.format("MERGE INTO %s as t\n", targetTable) +
                    String.format("USING (select %s  ) as  s\n", String.join(",", listUsingPart)) +
                    String.format("ON t.%s = s.%s \n", pkTargetName, pkField) +
                    "WHEN MATCHED AND dbz_op_type='d' THEN \n" +
                    "  DELETE\n" +
                    "WHEN MATCHED THEN \n  UPDATE SET " +
                    String.join(",", listUpdatePart) +
                    "\nWHEN NOT MATCHED THEN \n" +
                    String.format("INSERT (%s) \n", String.join(",", listInsertPart)) +
                    String.format("VALUES (%s) \n", String.join(",", listInsertValuesPart));

        }

        private void setSqlMirrorOracle(){

            List<String> listUsingPart = new ArrayList<>();
            List<String> listUpdatePart = new ArrayList<>();
            List<String> listInsertPart = new ArrayList<>();
            List<String> listInsertValuesPart = new ArrayList<>();
            String pkTargetName=this.pkField;
            for (Field field : fields) {
                String castVar = castOracle(field.type);
                listUsingPart.add(castVar + " as " + field.source_name);
                if (!Objects.equals(field.stateField, "common")) {

                    if (!Objects.equals(field.source_name, this.pkField)) {
                        listUpdatePart.add("\n  " + field.target_name + " = s." + field.source_name);
                    }
                    else
                    {
                        pkTargetName = field.target_name;
                    }


                    listInsertPart.add(field.target_name);
                    listInsertValuesPart.add("s." + field.source_name);
                }
            }




            this.sqlStatement= String.format("MERGE INTO %s  t\n", targetTable) +
                    String.format("USING (select %s  from dual)   s\n", String.join(",", listUsingPart)) +
                    String.format("ON (t.%s = s.%s) \n", pkTargetName, pkField) +
                    "WHEN MATCHED THEN\n" +
                    "  UPDATE SET \n" +
                    String.join(",", listUpdatePart) +
                    "\nDELETE WHERE (dbz_op_type='d') \n"+
                    "WHEN NOT MATCHED THEN \n" +
                    String.format("INSERT (%s) \n", String.join(",", listInsertPart)) +
                    String.format("VALUES (%s) \n", String.join(",", listInsertValuesPart));

        }

        private void setSqlLogPostgres(){


            List<String> listInsertPart = new ArrayList<>();
            List<String> listValuesPart = new ArrayList<>();

            for (Field field : fields) {
                listInsertPart.add(field.target_name);
                String castVar = castPostgres(field.type);
                listValuesPart.add(castVar);

            }


            StringBuilder sqlLog = new StringBuilder();
            sqlLog.append(String.format("INSERT INTO %s \n", targetTable));
            sqlLog.append(String.format("(%s)\n", String.join(",", listInsertPart)));
            sqlLog.append("VALUES \n");
            sqlLog.append(String.format("(%s)\n", String.join(",", listValuesPart)));
            System.out.println(sqlLog);
            this.sqlStatement= String.valueOf(sqlLog);
        }

        private void setSqlLogOracle(){


            List<String> listInsertPart = new ArrayList<>();
            List<String> listValuesPart = new ArrayList<>();

            for (Field field : fields) {
                listInsertPart.add(field.target_name);
                String castVar = castOracle(field.type);
                listValuesPart.add(castVar);

            }


            StringBuilder sqlLog = new StringBuilder();
            sqlLog.append(String.format("INSERT INTO %s \n", targetTable));
            sqlLog.append(String.format("(%s)\n", String.join(",", listInsertPart)));
            sqlLog.append("VALUES \n");
            sqlLog.append(String.format("(%s)\n", String.join(",", listValuesPart)));
            System.out.println(sqlLog);
            this.sqlStatement= String.valueOf(sqlLog);


        }

        private static String castPostgres(String fieldType) {
            String var = "";
            switch (fieldType) {
                case "timestamp":
                    var ="?::timestamp " ;
                    break;
                case "timestamptz":
                    var +="?::timestamptz ";
                    break;
                case "date":
                    var +="?::date ";
                    break;
                case "time":
                    var +="?::time ";
                    break;
                case "timetz":
                    var +="?::timetz ";
                    break;
                default:
                    var +="? ";
                    break;
            }
            return var;
        }

        private static String castOracle(String fieldType) {
            String var = "";
            switch (fieldType) {
                default:
                    var +="? ";
                    break;
            }
            return var;
        }

    }




    HashMap<String, List<Target>> tableProperties  = new HashMap<>();



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
            String tableName = table.asJsonObject().getString("schema")+"."+table.asJsonObject().getString("name");
            setTableProperties(table.asJsonObject(), tableName);
        }

    }










    public String getStringTableList(){
        return String.join(",", this.tableList);
    }




    private void setTableProperties(JsonObject table, String tableName ) {
        if (table.asJsonObject().containsKey("target")) {
            tableProperties.put(tableName, new ArrayList<>());
            for (JsonValue tableTarget : table.getJsonArray("target")) {


                tableProperties.get(tableName).add(new Target(
                        getJsonValue(tableTarget.asJsonObject(), "name"),
                        getJsonValue(tableTarget.asJsonObject(), "table"),
                        getJsonValue(tableTarget.asJsonObject(), "notation"),
                        getJsonValue(tableTarget.asJsonObject(), "type"),
                        getJsonList(tableTarget.asJsonObject(), "fields"),
                        getJsonList(tableTarget.asJsonObject(), "map"),
                        table.getJsonArray("fields"),
                        getJsonValue(table, "pk")

                ));
            }

        }




    }

    private static String getJsonValue(JsonObject obj, String key) {
        String value;
        if ( obj.containsKey(key)&&!obj.isNull(key))
            value= obj.getString(key);
        else
            value="" ;
        return value;
    }

    private static List<String> getJsonList(JsonObject obj, String key) {
        List<String> value= new ArrayList<>();
        if ( obj.containsKey(key)&&!obj.isNull(key))
            for (JsonValue val : obj.getJsonArray(key)) {
                value.add(val.toString().replace("\"",""));
            }

        return value;
    }


}
