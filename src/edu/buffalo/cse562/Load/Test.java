package edu.buffalo.cse562.Load;

public class Test {
	public static void main(String []args){
		String line = "133|7|5|2|12|12926.04|0.02|0.06|N|O|1997-12-02|1998-01-15";
		int i = 0;
		int j = 0;
		int count = 0;
		String col = null;
		StringBuilder temp = new StringBuilder(line);
		int index = 12;
		while (count < index && (line.indexOf("|", j) >= 0)) {
			i = line.indexOf("|", j);

			if (count == index-1)
				col = temp.substring(j, i);

			j = i + 1;
			count++;
		}
		if(col==null)
			col = temp.substring(j, line.length());
		
		System.out.println(col);
		
	}
}
