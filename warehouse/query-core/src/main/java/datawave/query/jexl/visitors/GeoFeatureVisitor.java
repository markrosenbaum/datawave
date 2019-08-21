package datawave.query.jexl.visitors;

import datawave.query.jexl.functions.GeoFunctionsDescriptor;
import datawave.query.jexl.functions.GeoWaveFunctionsDescriptor;
import datawave.query.jexl.functions.JexlFunctionArgumentDescriptorFactory;
import datawave.query.jexl.functions.arguments.JexlArgumentDescriptor;
import datawave.webservice.common.logging.ThreadConfigurableLogger;
import datawave.webservice.query.map.QueryGeometry;
import org.apache.commons.jexl2.parser.ASTFunctionNode;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.log4j.Logger;
import org.geotools.geojson.geom.GeometryJSON;
import org.locationtech.jts.io.WKTReader;

import java.util.LinkedHashSet;
import java.util.Set;

/**
 * This visitor will traverse the query tree, and extract both the geo function and associated query geometry (as GeoJSON).
 */
public class GeoFeatureVisitor extends BaseVisitor {
    private static final Logger log = ThreadConfigurableLogger.getLogger(GeoFeatureVisitor.class);
    
    private Set<QueryGeometry> geoFeatures;
    private GeometryJSON geoJson = new GeometryJSON();
    private WKTReader wktReader = new WKTReader();
    
    private boolean isLuceneQuery;
    
    private GeoFeatureVisitor(Set<QueryGeometry> geoFeatures) {
        this(geoFeatures, false);
    }
    
    private GeoFeatureVisitor(Set<QueryGeometry> geoFeatures, boolean isLuceneQuery) {
        this.geoFeatures = geoFeatures;
        this.isLuceneQuery = isLuceneQuery;
    }
    
    public static Set<QueryGeometry> getGeoFeatures(JexlNode node) {
        return getGeoFeatures(node, false);
    }
    
    public static Set<QueryGeometry> getGeoFeatures(JexlNode node, boolean isLuceneQuery) {
        Set<QueryGeometry> geoFeatures = new LinkedHashSet<>();
        node.jjtAccept(new GeoFeatureVisitor(geoFeatures, isLuceneQuery), null);
        return geoFeatures;
    }
    
    @Override
    public Object visit(ASTFunctionNode node, Object data) {
        JexlArgumentDescriptor desc = JexlFunctionArgumentDescriptorFactory.F.getArgumentDescriptor(node);
        
        try {
            if (desc instanceof GeoFunctionsDescriptor.GeoJexlArgumentDescriptor) {
                JexlNode geowaveNode = ((GeoFunctionsDescriptor.GeoJexlArgumentDescriptor) desc).toGeoWaveFunction(desc.fields(null, null));
                geowaveNode.jjtAccept(this, JexlStringBuildingVisitor.buildQuery(node));
            } else if (desc instanceof GeoWaveFunctionsDescriptor.GeoWaveJexlArgumentDescriptor) {
                QueryGeometry feature = new QueryGeometry();
                
                if (data != null)
                    feature.setFunction((String) data);
                else
                    feature.setFunction(JexlStringBuildingVisitor.buildQuery(node));
                
                // reformat as a lucene function
                if (isLuceneQuery) {
                    String function = feature.getFunction();
                    
                    int paramsIdx = function.indexOf('(');
                    String op = function.substring(0, function.indexOf('('));
                    String params = function.substring(paramsIdx);
                    
                    feature.setFunction(op.replace("geowave:", "#").toUpperCase() + params);
                }
                
                feature.setGeometry(geoJson.toString(wktReader.read(((GeoWaveFunctionsDescriptor.GeoWaveJexlArgumentDescriptor) desc).getWkt())));
                geoFeatures.add(feature);
            }
        } catch (Exception e) {
            log.error("Unable to extract geo feature from function", e);
        }
        
        return node;
    }
}
