package datawave.webservice.query.map;

import org.apache.commons.lang.StringEscapeUtils;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.io.Serializable;

@XmlRootElement
@XmlAccessorType(XmlAccessType.NONE)
public class QueryGeometry implements Serializable {
    
    @XmlElement
    private String function;
    @XmlElement
    private String geometry;
    
    public String getFunction() {
        return function;
    }
    
    public void setFunction(String function) {
        this.function = function;
    }
    
    public String getGeometry() {
        return geometry;
    }
    
    public void setGeometry(String geometry) {
        this.geometry = geometry;
    }
    
    String toGeoJsonFeature() {
        return "{'type': 'Feature', 'properties': {'function': \"" + StringEscapeUtils.escapeHtml(function) + "\"},'geometry': " + geometry + "}";
    }
    // TODO: implement hash, equals, copy, etc.
}
