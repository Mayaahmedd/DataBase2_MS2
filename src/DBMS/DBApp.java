package DBMS;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;

public class DBApp {
    static int dataPageSize = 2;

    public static void createTable(String tableName, String[] columnsNames) {
        Table t = new Table(tableName, columnsNames);
        FileManager.storeTable(tableName, t);
    }

    // public static void insert(String tableName, String[] record)
    // {
    // Table t = FileManager.loadTable(tableName);
    // t.insert(record);
    // FileManager.storeTable(tableName, t);
    // }

    public static ArrayList<String[]> select(String tableName) {
        Table t = FileManager.loadTable(tableName);
        ArrayList<String[]> res = t.select();
        FileManager.storeTable(tableName, t);
        return res;
    }

    public static ArrayList<String[]> select(String tableName, int pageNumber, int recordNumber) {
        Table t = FileManager.loadTable(tableName);
        ArrayList<String[]> res = t.select(pageNumber, recordNumber);
        FileManager.storeTable(tableName, t);
        return res;
    }

    public static ArrayList<String[]> select(String tableName, String[] cols, String[] vals) {
        Table t = FileManager.loadTable(tableName);
        ArrayList<String[]> res = t.select(cols, vals);
        FileManager.storeTable(tableName, t);
        return res;
    }

    public static String getFullTrace(String tableName) {
        Table t = FileManager.loadTable(tableName);
        String res = t.getFullTrace();
        return res;
    }

    public static String getLastTrace(String tableName) {
        Table t = FileManager.loadTable(tableName);
        String res = t.getLastTrace();
        return res;
    }


    public static ArrayList<String[]> validateRecords(String tableName) {
        Table t = FileManager.loadTable(tableName);
        if (t == null) return new ArrayList<>();
        
        ArrayList<String[]> missingRecords = new ArrayList<>();
        ArrayList<String[]> allExpectedRecords = t.select();
        
        // Build set of existing records
        HashSet<String> existingRecords = new HashSet<>();
        for (int i = 0; i < t.pageCount; i++) {
            Page p = FileManager.loadTablePage(tableName, i);
            if (p != null) {
                for (String[] record : p.select()) {
                    existingRecords.add(Arrays.toString(record));
                }
            }
        }
        
        // Find missing records
        for (String[] expected : allExpectedRecords) {
            if (!existingRecords.contains(Arrays.toString(expected))) {
                missingRecords.add(expected);
            }
        }
        
        // Update trace with exact required format
        t.trace.add("Validating records: " + missingRecords.size() + " records missing.");
        FileManager.storeTable(tableName, t);
        
        return missingRecords;
    }
    
    
    
    
    public static void recoverRecords(String tableName, ArrayList<String[]> missing) {
        Table t = FileManager.loadTable(tableName);
        if (t == null) {
            return;
        }

        ArrayList<Integer> recoveredPages = new ArrayList<>();
        int missingCount = (missing == null) ? 0 : missing.size();

        if (missing != null && !missing.isEmpty()) {
            // Group records by their original page numbers
            HashMap<Integer, ArrayList<String[]>> pageRecordsMap = new HashMap<>();

            // Get all records to determine original positions
            ArrayList<String[]> allRecords = t.select();

            // Create a map of record to its original position
            HashMap<String, Integer> recordPositions = new HashMap<>();
            for (int i = 0; i < allRecords.size(); i++) {
                recordPositions.put(Arrays.toString(allRecords.get(i)), i);
            }

            // Group missing records by their original page numbers
            for (String[] record : missing) {
                Integer pos = recordPositions.get(Arrays.toString(record));
                if (pos != null) {
                    int pageNum = pos / dataPageSize;
                    if (!pageRecordsMap.containsKey(pageNum)) {
                        pageRecordsMap.put(pageNum, new ArrayList<>());
                        recoveredPages.add(pageNum);
                    }
                    pageRecordsMap.get(pageNum).add(record);
                }
            }

            // Sort recovered pages to maintain order
            Collections.sort(recoveredPages);

            // Recover each page
            for (Integer pageNum : recoveredPages) {
                Page p = new Page();
                // Get all records that should be on this page
                int startIdx = pageNum * dataPageSize;
                int endIdx = Math.min(startIdx + dataPageSize, allRecords.size());

                // Add records in their original order
                for (int i = startIdx; i < endIdx; i++) {
                    String[] record = allRecords.get(i);
                    p.insert(record);
                }

                FileManager.storeTablePage(tableName, pageNum, p);
            }
        }

        // Always add a trace, even if nothing was recovered
        String traceMsg = "Recovering " + missingCount + " records in pages: " + recoveredPages;
        t.trace.add(traceMsg);
        FileManager.storeTable(tableName, t);
    }
    
    public static void createBitMapIndex(String tableName, String colName) {
        long startTime = System.currentTimeMillis();
        Table t = FileManager.loadTable(tableName);

        // Find column index
        int colIndex = -1;
        for (int i = 0; i < t.columnsNames.length; i++) {
            if (t.columnsNames[i].equals(colName)) {
                colIndex = i;
                break;
            }
        }
        if (colIndex == -1) return;

        // Get all records
        ArrayList<String[]> allRecords = t.select();
        BitmapIndex bitmapIndex = new BitmapIndex();
        bitmapIndex.updateSize(allRecords.size());

        // Create bitmap index
        for (int i = 0; i < allRecords.size(); i++) {
            String value = allRecords.get(i)[colIndex];
            bitmapIndex.insert(value, i);
        }

        // Save the index
        FileManager.storeTableIndex(tableName, colName, bitmapIndex);

        // Add the column to indexed columns list (automatically sorted in getFullTrace)
        t.addIndexedColumn(colName);

        long stopTime = System.currentTimeMillis();
        t.trace.add("Index created for column: " + colName + ", execution time (mil):" + (stopTime - startTime));
        FileManager.storeTable(tableName, t);
    }

    public static ArrayList<String[]> selectIndex(String tableName, String[] cols, String[] vals) {
        long startTime = System.currentTimeMillis();
        Table t = FileManager.loadTable(tableName);

        // Separate indexed and non-indexed columns
        ArrayList<String> indexedCols = new ArrayList<>();
        ArrayList<String> nonIndexedCols = new ArrayList<>();
        ArrayList<String> indexedVals = new ArrayList<>();
        ArrayList<String> nonIndexedVals = new ArrayList<>();

        for (int i = 0; i < cols.length; i++) {
            String col = cols[i];
            BitmapIndex index = FileManager.loadTableIndex(tableName, col);
            if (index != null) {
                indexedCols.add(col);
                indexedVals.add(vals[i]);
            } else {
                nonIndexedCols.add(col);
                nonIndexedVals.add(vals[i]);
            }
        }

        ArrayList<String[]> allRecords = t.select();
        ArrayList<String[]> result = new ArrayList<>();
        int indexedSelectionCount = 0;

        if (!indexedCols.isEmpty()) {
            BitSet finalBitSet = new BitSet(allRecords.size());
            finalBitSet.set(0, allRecords.size(), true);

            for (int i = 0; i < indexedCols.size(); i++) {
                String col = indexedCols.get(i);
                String val = indexedVals.get(i);

                BitmapIndex index = FileManager.loadTableIndex(tableName, col);
                BitSet currentBitSet = new BitSet();
                String bitString = index.getBitString(val);
                for (int j = 0; j < bitString.length(); j++) {
                    if (bitString.charAt(j) == '1') {
                        currentBitSet.set(j);
                    }
                }

                indexedSelectionCount = currentBitSet.cardinality();
                finalBitSet.and(currentBitSet);
            }

            for (int i = 0; i < allRecords.size(); i++) {
                if (finalBitSet.get(i)) {
                    result.add(allRecords.get(i));
                }
            }
        } else {
            result = t.select(cols, vals);
        }

        if (!nonIndexedCols.isEmpty()) {
            ArrayList<String[]> filteredResult = new ArrayList<>();
            for (String[] record : result) {
                boolean match = true;
                for (int i = 0; i < nonIndexedCols.size(); i++) {
                    String col = nonIndexedCols.get(i);
                    String val = nonIndexedVals.get(i);

                    int colIndex = -1;
                    for (int j = 0; j < t.columnsNames.length; j++) {
                        if (t.columnsNames[j].equals(col)) {
                            colIndex = j;
                            break;
                        }
                    }

                    if (colIndex == -1 || !record[colIndex].equals(val)) {
                        match = false;
                        break;
                    }
                }
                if (match) {
                    filteredResult.add(record);
                }
            }
            result = filteredResult;
        }

        // Sort columns lexicographically for trace
        Collections.sort(indexedCols);
        Collections.sort(nonIndexedCols);

        long stopTime = System.currentTimeMillis();
        String traceMsg = "Select index condition:" + Arrays.toString(cols) + "->" + Arrays.toString(vals);

        if (!indexedCols.isEmpty()) {
            traceMsg += ", Indexed columns: " + indexedCols;
            traceMsg += ", Indexed selection count: " + indexedSelectionCount;
        }
        if (!nonIndexedCols.isEmpty()) {
            traceMsg += ", Non Indexed: " + nonIndexedCols;
        }

        traceMsg += ", Final count: " + result.size();
        traceMsg += ", execution time (mil):" + (stopTime - startTime);

        t.trace.add(traceMsg);
        FileManager.storeTable(tableName, t);
        return result;
    }
    
    
   
    
    
    

    private static ArrayList<String> getIndexedColumns(String tableName) {
        Table t = FileManager.loadTable(tableName);
        return t.indexedColumns;
    }

    public static void insert(String tableName, String[] record) {
        Table t = FileManager.loadTable(tableName);

        // First insert the record
        long startTime = System.currentTimeMillis();
        Page current = FileManager.loadTablePage(tableName, t.pageCount - 1);
        if (current == null || !current.insert(record)) {
            current = new Page();
            current.insert(record);
            t.pageCount++;
        }
        FileManager.storeTablePage(tableName, t.pageCount - 1, current);
        t.recordsCount++;

        // Get all records including the new one
        ArrayList<String[]> allRecords = t.select();
        int newRecordPos = allRecords.size() - 1; // Position of the new record

        // Update all existing bitmap indexes
        for (String colName : t.columnsNames) {
            BitmapIndex index = FileManager.loadTableIndex(tableName, colName);
            if (index != null) {
                // Find column index
                int colIndex = -1;
                for (int i = 0; i < t.columnsNames.length; i++) {
                    if (t.columnsNames[i].equals(colName)) {
                        colIndex = i;
                        break;
                    }
                }
                if (colIndex == -1)
                    continue;

                // Update the index size
                index.updateSize(allRecords.size());

                // Get the value for this column in the new record
                String value = record[colIndex];

                // Update the bitmap for this value
                index.insert(value, newRecordPos);

                // Save the updated index
                FileManager.storeTableIndex(tableName, colName, index);
            }
        }

        long stopTime = System.currentTimeMillis();
        t.trace.add("Inserted: " + Arrays.toString(record) + ", at page number:" + (t.pageCount - 1)
                + ", execution time (mil):" + (stopTime - startTime));
        FileManager.storeTable(tableName, t);
    }
    
    
    public static void main(String[] args) throws IOException {
        FileManager.reset();
        String[] cols = { "id", "name", "major", "semester", "gpa" };
        createTable("student", cols);
        String[] r1 = { "1", "stud1", "CS", "5", "0.9" };
        insert("student", r1);

        String[] r2 = { "2", "stud2", "BI", "7", "1.2" };
        insert("student", r2);

        String[] r3 = { "3", "stud3", "CS", "2", "2.4" };
        insert("student", r3);

        createBitMapIndex("student", "gpa");
        createBitMapIndex("student", "major");

        System.out.println("Bitmap of the value of CS from the major index: " + getValueBits("student", "major", "CS"));
        System.out.println("Bitmap of the value of 1.2 from the gpa index: " + getValueBits("student", "gpa", "1.2"));

        String[] r4 = { "4", "stud4", "CS", "9", "1.2" };
        insert("student", r4);

        String[] r5 = { "5", "stud5", "BI", "4", "3.5" };
        insert("student", r5);

        System.out.println("After new insertions:");
        System.out.println("Bitmap of the value of CS from the major index: " + getValueBits("student", "major", "CS"));
        System.out.println("Bitmap of the value of 1.2 from the gpa index: " + getValueBits("student", "gpa", "1.2"));

        System.out.println("Output of selection using index when all columns of the select conditions are indexed:");
        ArrayList<String[]> result1 = selectIndex("student", new String[] { "major", "gpa" },
                new String[] { "CS", "1.2" });
        for (String[] array : result1) {
            for (String str : array) {
                System.out.print(str + " ");
            }
            System.out.println();
        }
        System.out.println("Last trace of the table: " + getLastTrace("student"));
        System.out.println("--------------------------------");

        System.out.println(
                "Output of selection using index when only one column of the columns of the select conditions are indexed:");
        ArrayList<String[]> result2 = selectIndex("student", new String[] { "major", "semester" },
                new String[] { "CS", "5" });
        for (String[] array : result2) {
            for (String str : array) {
                System.out.print(str + " ");
            }
            System.out.println();
        }
        System.out.println("Last trace of the table: " + getLastTrace("student"));
        System.out.println("--------------------------------");

        System.out.println(
                "Output of selection using index when some of the columns of the select conditions are indexed:");
        ArrayList<String[]> result3 = selectIndex("student", new String[] { "major", "semester", "gpa" },
                new String[] { "CS", "5", "0.9" });
        for (String[] array : result3) {
            for (String str : array) {
                System.out.print(str + " ");
            }
            System.out.println();
        }
        System.out.println("Last trace of the table: " + getLastTrace("student"));
        System.out.println("--------------------------------");
        System.out.println("Full Trace of the table:");
        System.out.println(getFullTrace("student"));
        System.out.println("--------------------------------");
        System.out.println("The trace of the Tables Folder:");
        System.out.println(FileManager.trace());
    }

    /**
     * Returns the bitstream representation of a value's locations in a bitmap index
     * @param tableName Name of the table
     * @param colName Name of the indexed column
     * @param value The value to look up in the index
     * @return String containing the bitstream (1s where value exists, 0s otherwise)
     */
    public static String getValueBits(String tableName, String colName, String value) {
        // Load the bitmap index for the specified column
        BitmapIndex bitmapIndex = FileManager.loadTableIndex(tableName, colName);
        
        if (bitmapIndex == null) {
            // If no index exists, return empty string or throw exception
            return "";
        }
        
        // Get the bit string representation for the value
        String bitString = bitmapIndex.getBitString(value);
        
        // Return the bit string (e.g., "1010" where 1 indicates presence of value)
        return bitString;
    }
}
