package WC;

import java.util.Comparator;

import org.apache.hadoop.io.Text;

public class tempo1  implements Comparator{
	public String artistName;
	public float tempo;
	
	public tempo1()
	{
		
	}
	
	
	@Override
	public int compare(Object o1, Object o2) {
		tempo1 t1=  (tempo1) o1;
		tempo1 t2=  (tempo1) o2;
		
		return  Float.compare(t2.tempo-t1.tempo, tempo);
		
	}



	public tempo1(String key, float tempo) {
		super();
		this.artistName = key;
		this.tempo = tempo;
	}
	
	

}
