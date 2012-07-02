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

import eu.stratosphere.pact.common.type.base.PactPair;
import eu.stratosphere.pact.common.type.base.PactString;

/**
 *
 * @author Robert Pagel <rob at clabs.cc>
 */
public class PactRecord extends PactPair<PactRecordKey, PactString> {

    public PactRecord() { super(); }
    
    public PactRecord(PactRecordKey key, PactString value) {
        super( key, value );
    }
    
    public PactRecordKey getRecordKey() {
        return this.getFirst();
    }
    
    public String getValue() {
        return this.getSecond().getValue();
    }
    
}