package com.linkedin.avroutil1.spotbugs;

import edu.umd.cs.findbugs.BugInstance;
import edu.umd.cs.findbugs.BugReporter;
import edu.umd.cs.findbugs.bcel.OpcodeStackDetector;
import org.apache.bcel.Const;

/**
 * detects direct instantiations of BinaryDecoder
 */
public class BinaryDecoderInstantiationDetector extends OpcodeStackDetector {
    private final BugReporter bugReporter;

    public BinaryDecoderInstantiationDetector(BugReporter bugReporter) {
        this.bugReporter = bugReporter;
    }

    @Override
    public void sawOpcode(int seen) {
        if (seen != Const.NEW) {
            return;
        }
        if (getClassConstantOperand().equals("org/apache/avro/io/BinaryDecoder")) {
            // new BinaryEncoder call
            BugInstance bug = new BugInstance(this, "BINARY_DECODER_INSTANTIATION", NORMAL_PRIORITY)
                    .addClassAndMethod(this)
                    .addSourceLine(this, getPC());
            bugReporter.reportBug(bug);
        }
    }
}
