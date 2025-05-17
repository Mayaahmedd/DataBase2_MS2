package DBMS;

import java.io.Serializable;
import java.util.BitSet;
import java.util.HashMap;

public class BitmapIndex implements Serializable {
    private HashMap<String, BitSet> indexMap;
    private int size;

    public BitmapIndex() {
        this.indexMap = new HashMap<>();
        this.size = 0;
    }

    public void insert(String value, int position) {
        BitSet bitSet = indexMap.computeIfAbsent(value, k -> new BitSet(size));
        bitSet.set(position);
        if (position >= size) {
            size = position + 1;
        }
    }

    public String getBitString(String value) {
        BitSet bitSet = indexMap.get(value);
        if (bitSet == null) {
            return getZeroBitString(size);
        }
        
        StringBuilder sb = new StringBuilder(size);
        for (int i = 0; i < size; i++) {
            sb.append(bitSet.get(i) ? "1" : "0");
        }
        return sb.toString();
    }

    private String getZeroBitString(int length) {
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            sb.append('0');
        }
        return sb.toString();
    }

    public void updateSize(int newSize) {
        if (newSize > size) {
            size = newSize;
        }
    }
}