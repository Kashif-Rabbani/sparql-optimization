package dk.uni.cs.utils;

import java.util.List;

class Runnable implements java.lang.Runnable {
    private Thread t;
    private final String threadName;
    private final List<String> classes;
    
    Runnable(String name, List<String> classes) {
        threadName = name;
        this.classes = classes;
        System.out.println("Creating " + threadName);
    }
    
    public void run() {
        System.out.println("Running " + threadName);
        try {
            this.classes.forEach(RDFSchemaExtractor::executeQueryOnGraphDB);
        } catch (Exception e) {
            System.out.println("Thread " + threadName + " interrupted.");
        }
        System.out.println("Thread " + threadName + " exiting.");
    }
    
    public void start() {
        System.out.println("Starting " + threadName);
        if (t == null) {
            t = new Thread(this, threadName);
            t.start();
        }
    }
}


