package main.java;

import java.util.Vector;

class PageAndRow {
    int page;
    int row;
    PageAndRow(int p, int r) {
        page = p;
        row = r;}

}

public class RowReference {
    Vector<PageAndRow> pageAndRow;
    Object x;
    Object y;
    Object z;

    public RowReference(Object x, Object y, Object z) {
        this.x = x;
        this.y = y;
        this.z = z;
    }

    public Object getX() {
        return x;
    }

    public Object getY() {
        return y;
    }

    public Object getZ() {
        return z;
    }

}
