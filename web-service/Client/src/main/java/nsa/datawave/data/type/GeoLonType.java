package nsa.datawave.data.type;

import nsa.datawave.data.normalizer.Normalizer;

public class GeoLonType extends BaseType<String> {
    
    private static final long serialVersionUID = 8912983433360105604L;
    
    public GeoLonType() {
        super(Normalizer.GEO_LON_NORMALIZER);
    }
    
    /**
     * Two String + normalizer reference
     * 
     * @return
     */
    @Override
    public long sizeInBytes() {
        return PrecomputedSizes.STRING_STATIC_REF * 2 + (2 * normalizedValue.length()) + (2 * delegate.length()) + Sizer.REFERENCE;
    }
}
