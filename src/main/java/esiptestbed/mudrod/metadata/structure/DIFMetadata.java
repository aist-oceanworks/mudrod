/*
 * Licensed under the Apache License, Version 2.0 (the "License"); you 
 * may not use this file except in compliance with the License. 
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package esiptestbed.mudrod.metadata.structure;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlRootElement(name="DIF", namespace="http://gcmd.gsfc.nasa.gov/Aboutus/xml/dif/")
public class DIFMetadata {
	@XmlElement(name = "Entry_ID")
	public String entry_id;

	@XmlElement(name = "Entry_Title")
	public String entry_title;
	
	@XmlElement(name = "Dataset_Series_Name")
	public String shortName;

	@XmlElement(name = "Summary")
	public DIFMetadata.Summary summary;
	
	@XmlElement(name = "ISO_Topic_Category")
	public String[] isoTopic;
	
	@XmlElement(name = "Sensor_Name")
	public DIFMetadata.Sensor sensor;
	
	@XmlElement(name = "Source_Name")
	public DIFMetadata.Source source;
	
	@XmlElement(name = "Project")
	public DIFMetadata.Project project;
	
	@XmlAccessorType(XmlAccessType.FIELD)
    public static class Summary {
        @XmlElement(name = "Abstract")
        public String Abstract;
	}
	
	@XmlAccessorType(XmlAccessType.FIELD)
    public static class Sensor {
        @XmlElement(name = "Short_Name")
        public String sensor_short_name;
        
        @XmlElement(name = "Long_Name")
        public String sensor_long_name;
	}
	
	@XmlAccessorType(XmlAccessType.FIELD)
    public static class Source {
        @XmlElement(name = "Short_Name")
        public String source_short_name;
        
        @XmlElement(name = "Long_Name")
        public String source_long_name;
	}
	
	@XmlAccessorType(XmlAccessType.FIELD)
    public static class Project {
        @XmlElement(name = "Short_Name")
        public String project_short_name;
        
        @XmlElement(name = "Long_Name")
        public String project_long_name;
	}
}
