package org.fao.fenix.d3p.process.impl;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;

public class Test {
    public static void main(String[] args) {
        Collection<String> data = Arrays.asList("1","2","3");
        for (Iterator<String> i = data.iterator(); i.hasNext();)
            System.out.print(i.next());
        for (Iterator<String> i=data.iterator(); i.hasNext();)
            System.out.print(i.next());
    }

}
