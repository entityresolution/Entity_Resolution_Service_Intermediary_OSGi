/*
 * Copyright 2013 SEARCH Group, Incorporated. 
 * 
 * See the NOTICE file distributed with  this work for additional information 
 * regarding copyright ownership.  SEARCH Group Inc. licenses this file to You
 * under the Apache License, Version 2.0 (the "License"); you may not use this 
 * file except in compliance with the License.  You may obtain a copy of the 
 * License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, 
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package gov.nij.bundles.intermediaries.ers.osgi;

import static org.junit.Assert.assertEquals;

import gov.nij.bundles.intermediaries.ers.osgi.AttributeParameters;
import gov.nij.bundles.intermediaries.ers.osgi.EntityResolutionConversionUtils;
import gov.nij.bundles.intermediaries.ers.osgi.EntityResolutionResults;
import gov.nij.bundles.intermediaries.ers.osgi.EntityResolutionService;
import gov.nij.bundles.intermediaries.ers.osgi.ExternallyIdentifiableRecord;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Before;
import org.junit.Test;

import serf.data.Attribute;

/**
 * This test class will test the Deterministic Factors algorithm. It will test the DF functions in isolation and then as part of a larger ER service call.
 * 
 */
public class EntityResolutionServiceDeterministicFactorsTest {

    private static final Log LOG = LogFactory.getLog(EntityResolutionServiceDeterministicFactorsTest.class);

    private static final String JARO_DISTANCE_IMPL = "com.wcohen.ss.Jaro";

    private EntityResolutionService service;

    private Set<AttributeParameters> attributeParametersSet;

    @Before
    public void setUp() throws Exception {
        service = new EntityResolutionService();
        attributeParametersSet = new HashSet<AttributeParameters>();
        AttributeParameters ap = new AttributeParameters("givenName");
        ap.setAlgorithmClassName(JARO_DISTANCE_IMPL);
        ap.setThreshold(0.8);
        attributeParametersSet.add(ap);

        ap = new AttributeParameters("sid");
        ap.setAlgorithmClassName(JARO_DISTANCE_IMPL);
        ap.setDeterminative(true);
        attributeParametersSet.add(ap);

        ap = new AttributeParameters("ssn");
        ap.setAlgorithmClassName(JARO_DISTANCE_IMPL);
        ap.setDeterminative(true);
        attributeParametersSet.add(ap);
    }

    @Test
    public void testSomeNullDeterministicAttributeMerge() throws Exception {

        // test for a bug found during demo on 5/2/2013

        List<ExternallyIdentifiableRecord> records;
        EntityResolutionResults results;
        List<ExternallyIdentifiableRecord> returnRecords;

        // Scenario 1

        records = new ArrayList<ExternallyIdentifiableRecord>();
        records.add(makeRecord("Andrew", null, "123456789", "record1"));
        records.add(makeRecord("Joe", null, "123456789", "record2"));

        results = service.resolveEntities(EntityResolutionConversionUtils.convertRecords(records), attributeParametersSet);
        returnRecords = EntityResolutionConversionUtils.convertRecordWrappers(results.getRecords());
        assertEquals(1, returnRecords.size());

    }

    @Test
    public void testDeterministicAttributeMergeWithNullEquality() throws Exception {

        // test for new requirements found during demo on 5/31/2013

        List<ExternallyIdentifiableRecord> records;
        EntityResolutionResults results;
        List<ExternallyIdentifiableRecord> returnRecords;

        records = new ArrayList<ExternallyIdentifiableRecord>();
        records.add(makeRecord("Andrew", null, "123456789", "record1"));
        records.add(makeRecord("Joe", "123", "123456789", "record2"));

        results = service.resolveEntities(EntityResolutionConversionUtils.convertRecords(records), attributeParametersSet);
        returnRecords = EntityResolutionConversionUtils.convertRecordWrappers(results.getRecords());
        assertEquals(1, returnRecords.size());

    }

    // The following tests resulted from a bug from mid-April 2013. The functionality
    // should be (for two records):
    // Scenario 1: Two records have identical and non-empty values for a
    // deterministic factor. Result: Match (and do no further ER)
    // Scenario 2: Two records have non-identical and non-empty values for a
    // deterministic factor. Result: Non-match (and do no further ER)
    // Scenario 3: Two records both have empty values for all deterministic
    // factors. Result: forward on to ER
    // Scenario 4: Record A has a non-empty value for all deterministic
    // factors, and Record B has an empty value for those factors.
    // Result: Do no further ER on Record A, and forward Record B on to ER.

    @Test
    public void testBasicDeterministicAttributeMergeScenario1() throws Exception {

        List<ExternallyIdentifiableRecord> records;
        EntityResolutionResults results;
        List<ExternallyIdentifiableRecord> returnRecords;

        // Scenario 1

        records = new ArrayList<ExternallyIdentifiableRecord>();
        records.add(makeRecord("Andrew", "123", "123456789", "record1"));
        records.add(makeRecord("Joe", "123", "123456789", "record2"));

        results = service.resolveEntities(EntityResolutionConversionUtils.convertRecords(records), attributeParametersSet);
        returnRecords = EntityResolutionConversionUtils.convertRecordWrappers(results.getRecords());
        assertEquals(1, returnRecords.size()); // because all deterministic
                                               // factors are present and equal
    }

    @Test
    public void testBasicDeterministicAttributeMergeScenario2() throws Exception {

        List<ExternallyIdentifiableRecord> records;
        EntityResolutionResults results;
        List<ExternallyIdentifiableRecord> returnRecords;

        // Scenario 2

        records = new ArrayList<ExternallyIdentifiableRecord>();
        records.add(makeRecord("Andrew", "123", "123456789", "record1"));
        records.add(makeRecord("Joe", "124", "123456789", "record2"));

        results = service.resolveEntities(EntityResolutionConversionUtils.convertRecords(records), attributeParametersSet);
        returnRecords = EntityResolutionConversionUtils.convertRecordWrappers(results.getRecords());
        assertEquals(2, returnRecords.size()); // because one deterministic
                                               // factor (sid) is different

        records = new ArrayList<ExternallyIdentifiableRecord>();
        records.add(makeRecord("Andrew", "123", "123456789", "record1"));
        records.add(makeRecord("Andruw", "124", "123456789", "record2"));

        results = service.resolveEntities(EntityResolutionConversionUtils.convertRecords(records), attributeParametersSet);
        returnRecords = EntityResolutionConversionUtils.convertRecordWrappers(results.getRecords());
        assertEquals(2, returnRecords.size()); // showing that even though ER
                                               // would normally merge these,
                                               // we don't merge because the
                                               // det factors are unequal
    }

    @Test
    public void testBasicDeterministicAttributeMergeScenario3() throws Exception {

        List<ExternallyIdentifiableRecord> records;
        EntityResolutionResults results;
        List<ExternallyIdentifiableRecord> returnRecords;

        // Scenario 3

        records = new ArrayList<ExternallyIdentifiableRecord>();
        records.add(makeRecord("Andrew", null, null, "record1"));
        records.add(makeRecord("Andruw", null, null, "record2"));

        results = service.resolveEntities(EntityResolutionConversionUtils.convertRecords(records), attributeParametersSet);
        returnRecords = EntityResolutionConversionUtils.convertRecordWrappers(results.getRecords());
        assertEquals(1, returnRecords.size()); // because we forward them on to
                                               // ER, and Andrew/Andruw is
                                               // close enough to merge

        records = new ArrayList<ExternallyIdentifiableRecord>();
        records.add(makeRecord("Andrew", null, null, "record1"));
        records.add(makeRecord("Joe", null, null, "record2"));

        results = service.resolveEntities(EntityResolutionConversionUtils.convertRecords(records), attributeParametersSet);
        returnRecords = EntityResolutionConversionUtils.convertRecordWrappers(results.getRecords());
        assertEquals(2, returnRecords.size()); // because we forward them on to
                                               // ER, and Andrew/Joe not close
                                               // enough to merge

    }

    @Test
    public void testBasicDeterministicAttributeMergeScenario4() throws Exception {

        List<ExternallyIdentifiableRecord> records;
        EntityResolutionResults results;
        List<ExternallyIdentifiableRecord> returnRecords;

        // Scenario 4

        records = new ArrayList<ExternallyIdentifiableRecord>();
        records.add(makeRecord("Andrew", null, null, "record1"));
        records.add(makeRecord("Andruw", null, null, "record2"));
        records.add(makeRecord("Andruw", "124", "123456789", "record3"));

        results = service.resolveEntities(EntityResolutionConversionUtils.convertRecords(records), attributeParametersSet);
        returnRecords = EntityResolutionConversionUtils.convertRecordWrappers(results.getRecords());
        assertEquals(2, returnRecords.size()); // because we forward on records
                                               // 1 and 2 to ER and they merge
                                               // (Andrew/Andruw close enuf)
                                               // and rec3 is separate because
                                               // of det factors

        records = new ArrayList<ExternallyIdentifiableRecord>();
        records.add(makeRecord("Andrew", null, null, "record1"));
        records.add(makeRecord("Joe", null, null, "record2"));
        records.add(makeRecord("Andruw", "124", "123456789", "record3"));

        results = service.resolveEntities(EntityResolutionConversionUtils.convertRecords(records), attributeParametersSet);
        returnRecords = EntityResolutionConversionUtils.convertRecordWrappers(results.getRecords());
        assertEquals(3, returnRecords.size()); // because we forward on records
                                               // 1 and 2 to ER and they dont
                                               // merge (Andrew/Joe not close
                                               // enuf) and rec3 is separate
                                               // because of det factors

    }

    /**
     * First confirm that the deterministally merged record set and then test the records that were not merged. After that, call the ER service so the RSwoosh ER implementation can run as well.
     * 
     * @throws Exception
     */
    @Test
    public void testDeterministicAttributeMerge() throws Exception {

        List<ExternallyIdentifiableRecord> records = new ArrayList<ExternallyIdentifiableRecord>();
        records.add(makeRecord("Andrew", "123", "123456789", "record1"));
        records.add(makeRecord("Andrew", "123", "123456789", "record2"));
        records.add(makeRecord("Andrew", "124", "123456999", "record3"));
        records.add(makeRecord("Andrew", "123", "123456789", "record4"));
        records.add(makeRecord("Andrew", "124", "123456789", "record5"));
        records.add(makeRecord("Andrew", "999", "999999999", "record6"));
        records.add(makeRecord("Andrew", null, null, "record7"));

        List<ExternallyIdentifiableRecord> mergedRecords = service.deterministicAttributeMerge(records, attributeParametersSet).getRecordsThatAreDeterministicallyMerged();
        LOG.debug("Return records from deterministic attribute merge:" + mergedRecords);
        assertEquals(4, mergedRecords.size());

        // Loop through the records and make sure record1, record2, and record4
        // are related
        for (ExternallyIdentifiableRecord extRecord : mergedRecords) {
            String recordID = extRecord.getExternalId();

            Set<String> relatedIDs = extRecord.getRelatedIds();

            if (recordID.equals("record1")) {
                assertEquals(true, relatedIDs.contains("record2"));
                assertEquals(true, relatedIDs.contains("record4"));
                assertEquals(2, relatedIDs.size());
            }

            if (recordID.equals("record2")) {
                assertEquals(true, relatedIDs.contains("record1"));
                assertEquals(true, relatedIDs.contains("record4"));
                assertEquals(2, relatedIDs.size());
            }

            if (recordID.equals("record4")) {
                assertEquals(true, relatedIDs.contains("record1"));
                assertEquals(true, relatedIDs.contains("record2"));
                assertEquals(2, relatedIDs.size());
            }
        }

        List<ExternallyIdentifiableRecord> recordsThatAreNotMerged = service.deterministicAttributeMerge(records, attributeParametersSet).getRecordsThatAreNotMerged();
        assertEquals(1, recordsThatAreNotMerged.size()); // will only have non-merged records now if all values of det factors are null
        ExternallyIdentifiableRecord extRecord = recordsThatAreNotMerged.toArray(new ExternallyIdentifiableRecord[] {})[0];
        assertEquals("record7", extRecord.getExternalId());

        // Now call the call 'resolveEntities' function
        EntityResolutionResults results = service.resolveEntities(EntityResolutionConversionUtils.convertRecords(records), attributeParametersSet);
        List<ExternallyIdentifiableRecord> returnRecords = EntityResolutionConversionUtils.convertRecordWrappers(results.getRecords());
        assertEquals(5, returnRecords.size());

    }

    private ExternallyIdentifiableRecord makeRecord(String givenName, String sid, String ssn, String recordId) {
        return new ExternallyIdentifiableRecord(makeAttributes(new Attribute("givenName", givenName), new Attribute("sid", sid), new Attribute("ssn", ssn)), recordId);
    }

    private static Map<String, Attribute> makeAttributes(Attribute... attributes) {
        Map<String, Attribute> ret = new HashMap<String, Attribute>();
        for (Attribute a : attributes) {
            ret.put(a.getType(), a);
        }
        return ret;
    }

}
