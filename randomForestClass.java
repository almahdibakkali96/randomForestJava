
// ---------- Java Util ---------
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.Scanner;

// ---------- Java IO ---------
import java.io.IOException;
import java.io.File;
import java.io.BufferedReader;
import java.io.FileReader;

// ---------- GridDB ---------
import com.toshiba.mwcloud.gs.Collection;
import com.toshiba.mwcloud.gs.ColumnInfo;
import com.toshiba.mwcloud.gs.Container;
import com.toshiba.mwcloud.gs.ContainerInfo;
import com.toshiba.mwcloud.gs.GSType;
import com.toshiba.mwcloud.gs.GridStore;
import com.toshiba.mwcloud.gs.GridStoreFactory;
import com.toshiba.mwcloud.gs.Query;
import com.toshiba.mwcloud.gs.Row;
import com.toshiba.mwcloud.gs.RowSet;


//----------- Weka ---------
import weka.core.Instances;
import weka.core.converters.ConverterUtils.DataSource;
import weka.classifiers.trees.RandomForest;
import weka.classifiers.Evaluation;




public class randomForestClass {

    public static void main(String[] args){
        try {

// Manage connection to GridDB
            Properties properties = new Properties();
            properties.setProperty("notificationAddress", "239.0.0.1");
            properties.setProperty("notificationPort", "31999");
            properties.setProperty("clusterName", "cluster");
            properties.setProperty("database", "public");
            properties.setProperty("user", "admin");
            properties.setProperty("password", "admin");
//Get Store and Container
            GridStore store = GridStoreFactory.getInstance().getGridStore(properties);
            
            store.getContainer("newContainer");

            String containerName = "mContainer";
        
// Define container schema and columns
        ContainerInfo containerInfo = new ContainerInfo();
        List<ColumnInfo> columnList = new ArrayList<ColumnInfo>();
        columnList.add(new ColumnInfo("key", GSType.INTEGER));
        columnList.add(new ColumnInfo("slenght", GSType.FLOAT));
        columnList.add(new ColumnInfo("swidth", GSType.FLOAT));
        columnList.add(new ColumnInfo("plenght", GSType.FLOAT));
        columnList.add(new ColumnInfo("pwidth", GSType.FLOAT));
        columnList.add(new ColumnInfo("irisclass", GSType.STRING));

        containerInfo.setColumnInfoList(columnList);
        containerInfo.setRowKeyAssigned(true);
        Collection<Void, Row> collection = store.putCollection(containerName, containerInfo, false);
        List<Row> rowList = new ArrayList<Row>();

// Handlig Dataset and storage to GridDB
            File data = new File("/home/ubuntu/griddb/gsSample/iris.csv");
            Scanner sc = new Scanner(data);  
            sc.useDelimiter("\n");
            while (sc.hasNext())  //returns a boolean value  
            {  
                int i = 0;
            Row row = collection.createRow();

            String line = sc.next();
            String columns[] = line.split(",");
            float slenght = Float.parseFloat(columns[0]);
            float swidth = Float.parseFloat(columns[1]);
            float plenght = Float.parseFloat(columns[2]);
            float pwidth = Float.parseFloat(columns[3]);
            String irisclass = columns[4];
                
            row.setInteger(0,i);
            row.setFloat(1,slenght );
            row.setFloat(2, swidth);
            row.setFloat(3, plenght);
            row.setFloat(4, pwidth);
            row.setString(5, irisclass);

            rowList.add(row);
    
            i++;
        }   
        
// Retrieving data from GridDB
        
        Container<?, Row> container = store.getContainer(containerName);

        if ( container == null ){
            throw new Exception("Container not found.");
        }
        Query<Row> query = container.query("SELECT * ");
        RowSet<Row> rowset = query.fetch(); // GOTO LINE 151
       

        int numFolds = 10;
        DataSource source = new DataSource("/home/ubuntu/griddb/gsSample/iris.csv");
        Instances datasetInstances = source.getDataSet();

        datasetInstances.setClassIndex(datasetInstances.numAttributes() - 1);
       
    
//Implement Random Forest Algorithm

String[] parameters = new String[14]; 
      
parameters[0] = "-P";
parameters[1] = "100";
parameters[2] = "-I";
parameters[3] = "100"; 
 parameters[4] = "-num-slots";
parameters[5] = "1"; 
parameters[6] = "-K";
parameters[7] = "0";
parameters[8] = "-M";
parameters[9] = "1.0";
parameters[10] = "-V";
parameters[11] = "0.001"; 
parameters[12] = "-S";
parameters[13] = "1"; 

RandomForest randomForest = new RandomForest();
randomForest.setOptions(parameters);

        randomForest.buildClassifier(datasetInstances);

        Evaluation evaluation = new Evaluation(datasetInstances);


        evaluation.crossValidateModel(randomForest, datasetInstances, numFolds, new Random(1));

        System.out.println(evaluation.toSummaryString("\nResults\n======\n", true));

     // Print GridDB data
        while ( rowset.hasNext() ) {
            Row row = rowset.next();
            float slenght = row.getFloat(0);
            float swidth = row.getFloat(1);
            float plenght = row.getFloat(2);
            float pwidth = row.getFloat(3);
            String irisclass = row.getString(4);
            System.out.println(" slenght=" + slenght + ", swidth=" + swidth + ", plenght=" + plenght +", pwidth=" + pwidth+", irisclass=" + irisclass);
        }

    // Terminating processes
  
        collection.put(rowList);
        sc.close();  //closes the scanner 
        rowset.close();
        query.close();
        container.close();
        store.close();
        System.out.println("success!");          
            

        } catch ( Exception e ){
            e.printStackTrace();
        }
    }


}