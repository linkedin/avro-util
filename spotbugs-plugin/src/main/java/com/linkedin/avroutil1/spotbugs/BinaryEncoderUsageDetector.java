package com.linkedin.avroutil1.spotbugs;

import edu.umd.cs.findbugs.BugInstance;
import edu.umd.cs.findbugs.BugReporter;
import edu.umd.cs.findbugs.bcel.OpcodeStackDetector;
import org.apache.bcel.Const;

/**
 * detects direct instantiations of BinaryEncoder
 */
public class BinaryEncoderUsageDetector extends OpcodeStackDetector {
    private final BugReporter bugReporter;

    public BinaryEncoderUsageDetector(BugReporter bugReporter) {
        this.bugReporter = bugReporter;
    }

    @Override
    public void sawOpcode(int seen) {
        if (seen != Const.NEW) {
            return;
        }
        if (getClassConstantOperand().equals("org/apache/avro/io/BinaryEncoder")) {
            // new BinaryEncoder call
            BugInstance bug = new BugInstance(this, "BINARY_ENCODER_INSTANTIATION", NORMAL_PRIORITY)
                    .addClassAndMethod(this)
                    .addSourceLine(this, getPC());
            bugReporter.reportBug(bug);
        }
    }
}
