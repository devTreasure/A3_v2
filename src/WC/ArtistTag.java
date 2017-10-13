package WC;

import java.util.Comparator;


public class ArtistTag implements Comparator {

	String genere;

	int tagRating;

	public ArtistTag()
	{
		
	}

	public ArtistTag( String genere, int rating) {
		super();

		this.genere = genere;
		this.tagRating = rating;
		
	}


	@Override
	public int compare(Object o1, Object o2) {
		ArtistTag a1=(ArtistTag)o1;
		ArtistTag a2=(ArtistTag)o2;
		
      return Integer.compare(a2.tagRating-a1.tagRating, tagRating); //
		
	}
}
