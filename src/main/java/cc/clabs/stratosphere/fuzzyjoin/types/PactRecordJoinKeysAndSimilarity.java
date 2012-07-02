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
package cc.clabs.stratosphere.fuzzyjoin.types;

import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactPair;

/**
 *
 * @author Robert Pagel <rob at clabs.cc>
 */
public class PactRecordJoinKeysAndSimilarity extends PactPair<PactRecordJoinKey, PactDouble> {

    public PactRecordJoinKeysAndSimilarity() { super(); }
    
    public PactRecordJoinKeysAndSimilarity(PactRecordKey a, PactRecordKey b, PactDouble sim) {
        super( new PactRecordJoinKey( a, b ), sim );
    }
    
    public PactRecordJoinKey getRecordJoinKey() {
        return this.getFirst();
    }
    
    public double getSimilarity() {
        return this.getSecond().getValue();
    }
    
}