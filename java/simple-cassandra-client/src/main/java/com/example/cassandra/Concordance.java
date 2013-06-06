package com.example.cassandra;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Concordance {
	private Map<String, List<String>> entries;

	public Concordance() {
		entries  = new HashMap<String, List<String>>();
	}
	
	public Map<String, List<String>> getEntries() {
		return entries;
	}

	public void analyzeText(File file, String abbreviation) {
		try {
			BufferedReader in = new BufferedReader(new FileReader(file));
			String line = "";
			int lineNumber = 1;
			while ( (line = in.readLine() ) != null ) {
				String annotatedLine = String.format("%s - %5d: %s", abbreviation, lineNumber++, line);
				for (String word : line.split("[ \t\n\r.,!?:;\"'()]")) {
					String lemma = word.toLowerCase();
					if ( entries.containsKey(lemma) ) {
						entries.get(lemma).add(annotatedLine);
					} else {
						List<String> contexts = new ArrayList<String>();
						contexts.add(annotatedLine);
						entries.put(lemma, contexts);
					}
				}
			}
			in.close();
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
   }
}
