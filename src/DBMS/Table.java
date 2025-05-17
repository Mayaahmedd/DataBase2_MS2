package DBMS;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

public class Table implements Serializable {
	public String name;
	public String[] columnsNames;
	public int pageCount;
	public int recordsCount;
	public ArrayList<String> trace;
	public ArrayList<String> indexedColumns;
	  public ArrayList<String[]> allRecordsCache;

	public Table(String name, String[] columnsNames) {
		this.name = name;
		this.columnsNames = columnsNames;
		this.trace = new ArrayList<>();
		this.indexedColumns = new ArrayList<>(); // Initialize the list
		this.trace.add("Table created name:" + name + ", columnsNames:"
				+ Arrays.toString(columnsNames));
	}
	
	
	
	public String getFullTrace() {
	    // Sort indexed columns lexicographically
	    Collections.sort(indexedColumns);
	    
	    StringBuilder res = new StringBuilder();
	    for (String entry : this.trace) {
	        res.append(entry).append("\n");
	    }
	    return res + "Pages Count: " + pageCount + ", Records Count: " + recordsCount +
	           ", Indexed Columns: " + indexedColumns;
	}
	
	  public ArrayList<String[]> getAllRecords() {
	        if (allRecordsCache == null) {
	            allRecordsCache = new ArrayList<>();
	            for (int i = 0; i < pageCount; i++) {
	                Page p = FileManager.loadTablePage(name, i);
	                if (p != null) {
	                    allRecordsCache.addAll(p.select());
	                }
	            }
	        }
	        return allRecordsCache;
	    }
	
	

	public void addIndexedColumn(String columnName) {
		if (!indexedColumns.contains(columnName)) {
			indexedColumns.add(columnName);
		}
	}



	@Override
	public String toString() {
		return "Table [name=" + name + ", columnsNames="
				+ Arrays.toString(columnsNames) + ", pageCount=" + pageCount
				+ ", recordsCount=" + recordsCount + "]";
	}

	public void insert(String[] record) {
		long startTime = System.currentTimeMillis();
		Page current = FileManager.loadTablePage(this.name, pageCount - 1);
		if (current == null || !current.insert(record)) {
			current = new Page();
			current.insert(record);
			pageCount++;
		}
		FileManager.storeTablePage(this.name, pageCount - 1, current);
		recordsCount++;
		long stopTime = System.currentTimeMillis();
		this.trace.add("Inserted:" + Arrays.toString(record) + ", at page number:" + (pageCount - 1)
				+ ", execution time (mil):" + (stopTime - startTime));
	}

	public String[] fixCond(String[] cols, String[] vals) {
		String[] res = new String[columnsNames.length];
		for (int i = 0; i < res.length; i++) {
			for (int j = 0; j < cols.length; j++) {
				if (columnsNames[i].equals(cols[j])) {
					res[i] = vals[j];
				}
			}
		}
		return res;
	}

	public ArrayList<String[]> select(String[] cols, String[] vals) {
		String[] cond = fixCond(cols, vals);
		String tracer = "Select condition:" + Arrays.toString(cols) + "->" + Arrays.toString(vals);
		ArrayList<ArrayList<Integer>> pagesResCount = new ArrayList<ArrayList<Integer>>();
		ArrayList<String[]> res = new ArrayList<String[]>();
		long startTime = System.currentTimeMillis();
		for (int i = 0; i < pageCount; i++) {
			Page p = FileManager.loadTablePage(this.name, i);
			ArrayList<String[]> pRes = p.select(cond);
			if (pRes.size() > 0) {
				ArrayList<Integer> pr = new ArrayList<Integer>();
				pr.add(i);
				pr.add(pRes.size());
				pagesResCount.add(pr);
				res.addAll(pRes);
			}
		}
		long stopTime = System.currentTimeMillis();
		tracer += ", Records per page:" + pagesResCount + ", records:" + res.size()
				+ ", execution time (mil):" + (stopTime - startTime);
		this.trace.add(tracer);
		return res;
	}

	public ArrayList<String[]> select(int pageNumber, int recordNumber) {
		String tracer = "Select pointer page:" + pageNumber + ", record:" + recordNumber;
		ArrayList<String[]> res = new ArrayList<String[]>();
		long startTime = System.currentTimeMillis();
		Page p = FileManager.loadTablePage(this.name, pageNumber);
		ArrayList<String[]> pRes = p.select(recordNumber);
		if (pRes.size() > 0) {
			res.addAll(pRes);
		}
		long stopTime = System.currentTimeMillis();
		tracer += ", total output count:" + res.size()
				+ ", execution time (mil):" + (stopTime - startTime);
		this.trace.add(tracer);
		return res;
	}

	public ArrayList<String[]> select() {
		ArrayList<String[]> res = new ArrayList<String[]>();
		long startTime = System.currentTimeMillis();
		for (int i = 0; i < pageCount; i++) {
			Page p = FileManager.loadTablePage(this.name, i);
			res.addAll(p.select());
		}
		long stopTime = System.currentTimeMillis();
		this.trace.add("Select all pages:" + pageCount + ", records:" + recordsCount
				+ ", execution time (mil):" + (stopTime - startTime));
		return res;
	}
	
	

	public String getLastTrace() {
		return this.trace.get(this.trace.size() - 1);
	}



}
