package esiptestbed.mudrod.ssearch.similarity;

public class GeoSimModel {
	private Bbox b1;
	private Bbox b2;

	public GeoSimModel(Bbox b1, Bbox b2)
	{
		this.b1 = b1;
		this.b2 = b2;
	}
	
	private double getOverlapArea()
	{
		double x_overlap = Math.max(0, Math.min(b1.right, b2.right) - Math.max(b1.left, b2.left));
		double y_overlap = Math.max(0, Math.min(b1.top, b2.top) - Math.max(b1.bottom, b2.bottom));
		return x_overlap * y_overlap;
	}
	
	private double getUnionArea()
	{
		return b1.getArea() + b2.getArea() - getOverlapArea();
	}
	
	public double getSimilarity()
	{
		double sim = 0.0;
		double area1 = b1.getArea();
		double area2 = b2.getArea();
		double overlap = getOverlapArea();
	    if ( area1> 0 && area2 > 0) {
	      sim = ( overlap / area1 + overlap / area2) * 0.5;
	    }
	    
	    return sim;
	}
	public static void main(String[] args) {
		// TODO Auto-generated method stub
        Bbox b1 = new Bbox(3.0, 1.0, 2.0, 0.0);
        Bbox b2 = new Bbox(2.0, 0.0, 3.0, 1.0);
        GeoSimModel model = new GeoSimModel(b1, b2);
        System.out.println(model.getSimilarity());    
	}

}
