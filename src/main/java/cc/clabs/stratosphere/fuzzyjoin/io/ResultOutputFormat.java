/**
 *  ______ __  __  ______  ______  __  __     __  ______  __  __   __
 * /\  ___/\ \/\ \/\___  \/\___  \/\ \_\ \   /\ \/\  __ \/\ \/\ "-.\ \
 * \ \  __\ \ \_\ \/_/  /_\/_/  /_\ \____ \ _\_\ \ \ \/\ \ \ \ \ \-.  \
 *  \ \_\  \ \_____\/\_____\/\_____\/\_____/\_____\ \_____\ \_\ \_\\"\_\
 *   \/_/   \/_____/\/_____/\/_____/\/_____\/_____/\/_____/\/_/\/_/ \/_/
 * 
 * ----------------------------------------------------------------------------
 * "THE BEER-WARE LICENSE" (Revision 42):
 * <rob@CLABS.CC> wrote this file. As long as you retain this notice you
 * can do whatever you want with this stuff. If we meet some day, and you think
 * this stuff is worth it, you can buy me a beer in return.
 * ----------------------------------------------------------------------------
 */
package cc.clabs.stratosphere.fuzzyjoin.io;

import cc.clabs.stratosphere.fuzzyjoin.types.PactRecord;
import cc.clabs.stratosphere.fuzzyjoin.types.PactRecordAndRecord;
import cc.clabs.stratosphere.fuzzyjoin.types.PactRecordKey;
import eu.stratosphere.pact.common.io.TextOutputFormat;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.base.PactDouble;

/**
 * 
 * @author Robert Pagel <rob at clabs.cc>
 */
public class ResultOutputFormat extends TextOutputFormat<PactDouble, PactRecordAndRecord> {
    
    @Override
    public byte[] writeLine( KeyValuePair<PactDouble, PactRecordAndRecord> pair ) {
        Double sim = pair.getKey().getValue();
        PactRecord record1 = pair.getValue().getFirst();
        PactRecord record2 = pair.getValue().getSecond();
        PactRecordKey key1 = record1.getRecordKey();
        PactRecordKey key2 = record2.getRecordKey();
        
        String id1 = key1.DATASET_NAME + key1.RECORD_ID;
        String id2 = key2.DATASET_NAME + key2.RECORD_ID;
        
        return String.format(
                "%.2f %s/%s ( %s | %s )\n",
                sim,
                id1,
                id2,
                record1.getValue(),
                record2.getValue()
            ).getBytes();
    }
}