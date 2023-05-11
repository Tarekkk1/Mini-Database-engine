package main.java;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Hashtable;
import java.util.Vector;

public class IndexMethods {

    public static void updateMetadata(String tableName, String[] colStrings)
            throws IOException, ParseException, DBAppException {
        FileReader metadata = new FileReader("src/main/resources/metadata.csv");
        BufferedReader br = new BufferedReader(metadata);
        String tableIndex = colStrings[0] +  colStrings[1] + colStrings[2] + "Index";
        StringBuilder metaDatanew = new StringBuilder();

        String curLine;
        FileWriter metaDataFile = new FileWriter("src/main/resources/metadata.csv");

        while ((curLine = br.readLine()) != null) {
            String[] curLineSplit = curLine.split(",");
            if (curLineSplit[0].equals(tableName) && contains(colStrings,curLineSplit[1])) {
                curLineSplit[4] = tableIndex;
                curLineSplit[5] = "Octree";
            }
            metaDatanew.append(curLineSplit.toString()).append("\n");
        }
        System.out.println(metaDatanew.toString());
        metaDataFile.write(metaDatanew.toString());
        metaDataFile.close();
    }

    private static boolean contains(String[]a, String s){
        for (String i : a){
            if (i.equals(s)){
                return true;
            }
        }
        return false;

    }

    public static void createIndex(String strTableName, String[] ColName)
            throws DBAppException, ClassNotFoundException, IOException, ParseException {

        // to check if the input is correct

        if (ColName.length != 3) {
            throw new DBAppException("Error in input");
        }

        String path;
        path = "src/main/resources/data/" + strTableName + ".ser";
        Table table = updateMethods.getTablefromCSV(path);
        table.index1 = ColName[0];
        table.index2 = ColName[1];
        table.index3 = ColName[2];
        deleteFromMethods.serialize(table, path);
        Object[] tableInfo = updateMethods.getTableInfoMeta(strTableName);
        Boundaries boundaries = new Boundaries();

        Hashtable<String, Object> columnMin = (Hashtable<String, Object>) tableInfo[1];
        Hashtable<String, Object> columnMax = (Hashtable<String, Object>) tableInfo[2];

        boundaries.minX = columnMin.get(ColName[0]);
        boundaries.maxX = columnMax.get(ColName[0]);
        boundaries.minY = columnMin.get(ColName[1]);
        boundaries.maxY = columnMax.get(ColName[1]);
        boundaries.minZ = columnMin.get(ColName[2]);
        boundaries.maxZ = columnMax.get(ColName[2]);

        Node root = new Node(boundaries, insertMethods.readConfig()[1]);

        for (int i = 0; i < table.getPages().size(); i++) {

            String pagePath = "src/main/resources/data/" + strTableName + i + ".ser";
            Vector<Hashtable<String, Object>> records = updateMethods.getPagesfromCSV(pagePath);
            for (int j = 0; j < records.size(); j++) {
                Object x = records.get(j).get(ColName[0]);
                Object y = records.get(j).get(ColName[1]);
                Object z = records.get(j).get(ColName[2]);
                root.insert(j, i, x, y, z);
            }
        }

        String indexPath = "src/main/resources/data/" + strTableName + "index.ser";
        deleteFromMethods.serialize(root, indexPath);
        updateMetadata(strTableName, ColName);
    }

    public static void updateIndex(String strTableName)
            throws ClassNotFoundException, IOException, DBAppException, ParseException {
        String path = "src/main/resources/data/" + strTableName + ".ser";
        Table table = updateMethods.getTablefromCSV(path);
        if (table.index1 != null) {
            File f = new File("src/main/resources/data/" + strTableName + "index.ser");
            f.delete();
            String[] ColName = new String[] { table.index1, table.index2, table.index3 };
            table.index1 = null;
            table.index2 = null;
            table.index3 = null;
            deleteFromMethods.serialize(table, path);

            createIndex(strTableName, ColName);

        }
        return;

    }

}
